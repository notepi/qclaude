"""
信号验证模块 v1.0
验证当前系统输出的信号是否有效

目标：回答"某天给出的信号，后续到底表现如何？"

验证逻辑：
- 读取历史 archive/metrics/*.parquet
- 计算次日/3日收益
- 按信号分组统计平均收益

用法：
    python3 src/validate_signals.py
    python3 src/validate_signals.py --output-json
    python3 src/validate_signals.py --min-samples 5
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import timedelta

import pandas as pd
import numpy as np

# 项目路径
PROJECT_ROOT = Path(__file__).parent.parent
ARCHIVE_METRICS_DIR = PROJECT_ROOT / "archive" / "metrics"
ANALYTICS_DIR = PROJECT_ROOT / "data" / "analytics"
NORMALIZED_PATH = PROJECT_ROOT / "data" / "normalized" / "market_data_normalized.parquet"

ANCHOR = "688333.SH"


def load_archive_metrics() -> pd.DataFrame:
    """读取所有历史 archive/metrics 文件"""
    if not ARCHIVE_METRICS_DIR.exists():
        raise FileNotFoundError(f"archive 目录不存在: {ARCHIVE_METRICS_DIR}")
    
    files = sorted(ARCHIVE_METRICS_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("archive/metrics 目录为空")
    
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"[WARN] 读取 {f.name} 失败: {e}")
    
    if not dfs:
        raise FileNotFoundError("无法读取任何 archive 文件")
    
    combined = pd.concat(dfs, ignore_index=True)
    
    # 确保日期格式
    if combined["trade_date"].dtype.kind != "M":
        combined["trade_date"] = pd.to_datetime(combined["trade_date"])
    
    combined = combined.sort_values("trade_date").reset_index(drop=True)
    print(f"[INFO] 读取 {len(combined)} 条 archive 记录")
    return combined


def load_price_series() -> pd.Series:
    """加载锚定股票的收盘价序列"""
    if not NORMALIZED_PATH.exists():
        raise FileNotFoundError(f"normalized 数据不存在: {NORMALIZED_PATH}")
    
    df = pd.read_parquet(NORMALIZED_PATH)
    anchor_df = df[df["ts_code"] == ANCHOR].copy()
    anchor_df = anchor_df.sort_values("trade_date").reset_index(drop=True)
    
    if anchor_df["trade_date"].dtype.kind != "M":
        anchor_df["trade_date"] = pd.to_datetime(anchor_df["trade_date"])
    
    price_series = anchor_df.set_index("trade_date")["close"]
    print(f"[INFO] 加载锚定股票 {ANCHOR} {len(price_series)} 个交易日价格")
    return price_series


def add_forward_returns(df: pd.DataFrame, price_series: pd.Series) -> pd.DataFrame:
    """
    添加未来收益列
    
    Returns:
        添加 columns:
        - t1_return: 次日涨跌幅
        - t3_return: 未来3日累计涨跌幅
    """
    df = df.copy()
    
    # 将 price_series 转为 dict 以便快速查找
    price_dict = price_series.to_dict()
    
    t1_returns = []
    t3_returns = []
    
    for idx, row in df.iterrows():
        current_date = row["trade_date"]
        
        # 找下一个交易日
        next_date = current_date + timedelta(days=1)
        found_next = False
        for i in range(1, 10):  # 最多找10天
            check_date = current_date + timedelta(days=i)
            if check_date in price_dict and price_dict[check_date] > 0:
                next_date = check_date
                found_next = True
                break
        
        # t1_return
        if found_next and current_date in price_dict and price_dict[current_date] > 0:
            t1 = (price_dict[next_date] - price_dict[current_date]) / price_dict[current_date]
            t1_returns.append(t1)
        else:
            t1_returns.append(np.nan)
        
        # t3_return: 未来3日累计
        if found_next:
            future_dates = [next_date + timedelta(days=i) for i in range(3)]
            valid_futures = [price_dict[d] for d in future_dates if d in price_dict and price_dict[d] > 0]
            if len(valid_futures) >= 1 and price_dict[current_date] > 0:
                t3 = (valid_futures[0] - price_dict[current_date]) / price_dict[current_date]
                t3_returns.append(t3)
            else:
                t3_returns.append(np.nan)
        else:
            t3_returns.append(np.nan)
    
    df["t1_return"] = t1_returns
    df["t3_return"] = t3_returns
    
    print(f"[INFO] 添加收益列: t1 有效 {df['t1_return'].notna().sum()}, t3 有效 {df['t3_return'].notna().sum()}")
    return df


def validate_categorical_signal(df: pd.DataFrame, signal_col: str, return_col: str = "t1_return") -> pd.DataFrame:
    """
    验证分类信号
    
    按 signal_col 分组，计算 return_col 的均值、样本数
    """
    results = []
    
    for signal_value in df[signal_col].dropna().unique():
        subset = df[df[signal_col] == signal_value][return_col].dropna()
        
        if len(subset) >= 2:  # 至少2个样本
            results.append({
                "signal": signal_value,
                "signal_col": signal_col,
                "sample_count": len(subset),
                "avg_return": subset.mean(),
                "std_return": subset.std(),
                "win_rate": (subset > 0).mean(),
            })
    
    return pd.DataFrame(results).sort_values("signal")


def validate_numerical_signal(df: pd.DataFrame, signal_col: str, return_col: str = "t1_return") -> pd.DataFrame:
    """
    验证数值信号
    
    按 >0 vs <=0 分组，对比收益
    """
    df_valid = df[df[signal_col].notna() & df[return_col].notna()].copy()
    
    if len(df_valid) < 2:
        return pd.DataFrame()
    
    positive = df_valid[df_valid[signal_col] > 0][return_col]
    negative = df_valid[df_valid[signal_col] <= 0][return_col]
    
    results = []
    
    if len(positive) >= 2:
        results.append({
            "signal": f"{signal_col} > 0",
            "signal_col": signal_col,
            "sample_count": len(positive),
            "avg_return": positive.mean(),
            "std_return": positive.std(),
            "win_rate": (positive > 0).mean(),
        })
    
    if len(negative) >= 2:
        results.append({
            "signal": f"{signal_col} <= 0",
            "signal_col": signal_col,
            "sample_count": len(negative),
            "avg_return": negative.mean(),
            "std_return": negative.std(),
            "win_rate": (negative > 0).mean(),
        })
    
    return pd.DataFrame(results)


def run_validation(min_samples: int = 2) -> pd.DataFrame:
    """运行完整验证"""
    print("=" * 60)
    print("信号验证 v1.0")
    print("=" * 60)
    
    # 1. 加载数据
    archive_df = load_archive_metrics()
    price_series = load_price_series()
    
    # 2. 添加收益列
    df = add_forward_returns(archive_df, price_series)
    
    # 保存带收益的原始数据
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ANALYTICS_DIR / "signal_validation_data.parquet", index=False)
    print(f"[INFO] 原始数据已保存至: {ANALYTICS_DIR / 'signal_validation_data.parquet'}")
    
    # 3. 验证分类信号
    categorical_signals = [
        "overall_signal_label",
        "momentum_label", 
        "capital_flow_trend_label",
        "price_strength_label",
        "volume_strength_label",
    ]
    
    all_results = []
    
    print()
    print("=" * 60)
    print("分类信号验证（次日收益）")
    print("=" * 60)
    
    for signal in categorical_signals:
        if signal in df.columns:
            result = validate_categorical_signal(df, signal, "t1_return")
            if not result.empty:
                result = result[result["sample_count"] >= min_samples]
                if not result.empty:
                    all_results.append(result)
                    print(f"\n【{signal}】")
                    for _, row in result.iterrows():
                        print(f"  {row['signal']:<20} 样本={row['sample_count']:>3}  "
                              f"平均={row['avg_return']:>+7.2%}  "
                              f"胜率={row['win_rate']:>6.1%}")
    
    # 4. 验证数值信号
    numerical_signals = [
        "relative_strength",
        "research_relative_strength",
    ]
    
    print()
    print("=" * 60)
    print("数值信号验证（次日收益）")
    print("=" * 60)
    
    for signal in numerical_signals:
        if signal in df.columns:
            result = validate_numerical_signal(df, signal, "t1_return")
            if not result.empty:
                result = result[result["sample_count"] >= min_samples]
                if not result.empty:
                    all_results.append(result)
                    print(f"\n【{signal}】")
                    for _, row in result.iterrows():
                        print(f"  {row['signal']:<20} 样本={row['sample_count']:>3}  "
                              f"平均={row['avg_return']:>+7.2%}  "
                              f"胜率={row['win_rate']:>6.1%}")
    
    # 5. 汇总保存
    if all_results:
        summary_df = pd.concat(all_results, ignore_index=True)
        summary_df.to_parquet(ANALYTICS_DIR / "signal_validation_summary.parquet", index=False)
        print(f"\n[INFO] 验证摘要已保存至: {ANALYTICS_DIR / 'signal_validation_summary.parquet'}")
    
    return summary_df


def main():
    parser = argparse.ArgumentParser(description="信号验证工具 v1.0")
    parser.add_argument("--min-samples", type=int, default=2, help="最小样本数")
    parser.add_argument("--output-json", action="store_true", help="JSON 格式输出")
    args = parser.parse_args()
    
    try:
        summary = run_validation(min_samples=args.min_samples)
        
        if args.output_json:
            print("\n" + "=" * 60)
            print("JSON 输出")
            print("=" * 60)
            print(summary.to_json(orient="records", force_ascii=False, indent=2))
            
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
