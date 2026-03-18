"""
评分层 v1.0
把多标签压缩成一个可验证的总分

用法：
    python3 src/score_layer.py
    python3 src/score_layer.py --validate  # 验证历史表现
    python3 src/score_layer.py --date 20260317

评分规则：
    - overall_signal_label: 强+2 / 中性偏强+1 / 中性0 / 中性偏弱-1 / 弱-2
    - price_strength_label: 强+1 / 中0 / 弱-1
    - volume_strength_label: 强+1 / 中0 / 弱-1
    - momentum_label: 价强量稳+1 / 短期震荡0 / 量价齐弱-1
    - capital_flow_trend_label: 资金持续流入+1 / 资金状态中性0 / 资金持续流出-1

评级：
    >= +3: 偏强
    +1 ~ +2: 中性偏强
    0: 中性
    -1 ~ -2: 中性偏弱
    <= -3: 偏弱
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent
ARCHIVE_DIR = PROJECT_ROOT / "archive" / "metrics"
ANALYTICS_DIR = PROJECT_ROOT / "data" / "analytics"

# 评分映射表
SCORE_MAP = {
    "overall_signal_label": {
        "强": 2,
        "中性偏强": 1,
        "中性": 0,
        "中性偏弱": -1,
        "弱": -2,
    },
    "price_strength_label": {
        "强": 1,
        "中": 0,
        "弱": -1,
    },
    "volume_strength_label": {
        "强": 1,
        "中": 0,
        "弱": -1,
    },
    "momentum_label": {
        "价强量稳": 1,
        "短期震荡": 0,
        "量价齐弱": -1,
    },
    "capital_flow_trend_label": {
        "资金持续流入": 1,
        "资金状态中性": 0,
        "资金持续流出": -1,
    },
}


def calc_score(row: pd.Series) -> dict:
    """
    计算单日评分
    
    Returns:
        {
            "signal_score": int,
            "signal_rating": str,
            "signal_breakdown": str,
        }
    """
    total = 0
    breakdown_parts = []
    
    for col, mapping in SCORE_MAP.items():
        val = row.get(col)
        if pd.isna(val) or val is None:
            continue
        
        score = mapping.get(str(val), 0)
        total += score
        
        if score != 0:
            sign = "+" if score > 0 else ""
            breakdown_parts.append(f"{col.replace('_label', '')}={val}({sign}{score})")
    
    # 评级
    if total >= 3:
        rating = "偏强"
    elif total >= 1:
        rating = "中性偏强"
    elif total == 0:
        rating = "中性"
    elif total >= -2:
        rating = "中性偏弱"
    else:
        rating = "偏弱"
    
    return {
        "signal_score": total,
        "signal_rating": rating,
        "signal_breakdown": "; ".join(breakdown_parts) if breakdown_parts else "无显著信号",
    }


def load_archive_metrics() -> pd.DataFrame:
    """加载所有 archive metrics"""
    files = sorted(ARCHIVE_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("archive/metrics 目录为空")
    
    dfs = [pd.read_parquet(f) for f in files if pd.read_parquet(f).shape[0] > 0]
    combined = pd.concat(dfs, ignore_index=True)
    
    if combined["trade_date"].dtype.kind != "M":
        combined["trade_date"] = pd.to_datetime(combined["trade_date"])
    
    return combined.sort_values("trade_date").reset_index(drop=True)


def add_score_to_df(df: pd.DataFrame) -> pd.DataFrame:
    """为 DataFrame 添加评分列"""
    scores = []
    for _, row in df.iterrows():
        scores.append(calc_score(row))
    
    score_df = pd.DataFrame(scores)
    return pd.concat([df, score_df], axis=1)


def validate_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    验证评分的历史表现
    
    需要 df 包含 t1_return 列
    """
    if "t1_return" not in df.columns:
        print("[WARN] 缺少 t1_return 列，无法验证")
        return pd.DataFrame()
    
    results = []
    
    for rating in df["signal_rating"].unique():
        subset = df[df["signal_rating"] == rating]["t1_return"].dropna()
        if len(subset) >= 2:
            results.append({
                "signal_rating": rating,
                "sample_count": len(subset),
                "avg_t1_return": subset.mean(),
                "win_rate": (subset > 0).mean(),
                "std_t1_return": subset.std(),
            })
    
    # 按评分从高到低排序
    rating_order = {"偏强": 5, "中性偏强": 4, "中性": 3, "中性偏弱": 2, "偏弱": 1}
    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df["_order"] = result_df["signal_rating"].map(rating_order)
        result_df = result_df.sort_values("_order", ascending=False).drop("_order", axis=1)
    
    return result_df


def main():
    parser = argparse.ArgumentParser(description="评分层 v1.0")
    parser.add_argument("--validate", action="store_true", help="验证历史表现")
    parser.add_argument("--date", type=str, help="计算指定日期评分 (YYYYMMDD)")
    args = parser.parse_args()
    
    print("=" * 60)
    print("评分层 v1.0")
    print("=" * 60)
    
    # 加载数据
    df = load_archive_metrics()
    print(f"[INFO] 加载 {len(df)} 条历史记录")
    
    # 计算评分
    df = add_score_to_df(df)
    
    # 保存
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ANALYTICS_DIR / "scored_metrics.parquet", index=False)
    print(f"[INFO] 评分结果已保存: {ANALYTICS_DIR / 'scored_metrics.parquet'}")
    
    # 显示最新评分
    if args.date:
        target_date = pd.to_datetime(args.date)
        latest = df[df["trade_date"] == target_date]
    else:
        latest = df.tail(1)
    
    if not latest.empty:
        row = latest.iloc[0]
        print(f"\n{'='*60}")
        print(f"最新评分 ({row['trade_date'].strftime('%Y-%m-%d')})")
        print(f"{'='*60}")
        print(f"总分: {row['signal_score']} ({row['signal_rating']})")
        print(f"分解: {row['signal_breakdown']}")
    
    # 验证
    if args.validate:
        # 需要加载带收益的验证数据
        try:
            validation_df = pd.read_parquet(ANALYTICS_DIR / "signal_validation_data.parquet")
            # 合并评分
            validation_df = add_score_to_df(validation_df)
            
            print(f"\n{'='*60}")
            print("评分验证 (t+1 收益)")
            print(f"{'='*60}")
            
            result = validate_score(validation_df)
            if not result.empty:
                for _, row in result.iterrows():
                    print(f"\n【{row['signal_rating']}】")
                    print(f"  样本数: {row['sample_count']}")
                    print(f"  次日平均: {row['avg_t1_return']:+.2%}")
                    print(f"  胜率: {row['win_rate']:.1%}")
                    print(f"  标准差: {row['std_t1_return']:.2%}")
                
                # 保存验证结果
                result.to_parquet(ANALYTICS_DIR / "score_validation.parquet", index=False)
                print(f"\n[INFO] 验证结果已保存: {ANALYTICS_DIR / 'score_validation.parquet'}")
            else:
                print("[WARN] 验证样本不足")
        except FileNotFoundError:
            print("[WARN] 未找到 signal_validation_data.parquet，请先运行 validate_signals.py")


if __name__ == "__main__":
    main()
