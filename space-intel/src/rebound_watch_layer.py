"""
反抽观察层 v1.0
识别"当前状态偏弱，但可能进入短线反抽观察区"的情形

用法：
    python3 src/rebound_watch_layer.py
    python3 src/rebound_watch_layer.py --date 20260317

原则：
    - 评分层描述"当前状态"
    - 反抽观察层只描述"是否值得观察反抽"
    - 不输出确定性预测

规则：
    基础触发：signal_rating=偏弱/弱, price_strength=弱
    辅助确认：资金未恶化、量能可控、rs显著为负、近5日回撤后

输出：
    - rebound_watch_flag: true/false
    - rebound_watch_level: low/medium/high
    - rebound_watch_reason: 简短解释
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
ANALYTICS_DIR = PROJECT_ROOT / "data" / "analytics"


def load_scored_metrics(date_str: str = None) -> dict:
    """加载评分数据"""
    df = pd.read_parquet(ANALYTICS_DIR / "scored_metrics.parquet")
    
    if date_str:
        target = pd.to_datetime(date_str)
        row = df[df["trade_date"] == target]
    else:
        row = df.tail(1)
    
    if row.empty:
        return None
    
    return row.iloc[0].to_dict()


def calc_rebound_watch(data: dict) -> dict:
    """
    计算反抽观察标记
    
    Returns:
        {
            "rebound_watch_flag": bool,
            "rebound_watch_level": str,  # low/medium/high
            "rebound_watch_reason": str,
        }
    """
    score = data.get("signal_score", 0)
    rating = data.get("signal_rating", "")
    overall = data.get("overall_signal_label", "")
    price = data.get("price_strength_label", "")
    volume = data.get("volume_strength_label", "")
    capital = data.get("capital_flow_trend_label", "")
    rs = data.get("relative_strength", 0) or 0
    anchor_return = data.get("anchor_return", 0) or 0
    
    # 近5日趋势
    price_trend_5d = data.get("price_trend_label", "")
    momentum_5d = data.get("momentum_label", "")
    
    # ========== 基础触发条件 ==========
    base_triggers = []
    
    # 1. 评分偏弱
    if rating in ["偏弱", "弱"]:
        base_triggers.append("评分偏弱")
    
    # 2. 综合信号弱
    if overall == "弱":
        base_triggers.append("综合信号弱")
    
    # 3. 价格强度弱
    if price == "弱":
        base_triggers.append("价格弱势")
    
    # 基础触发至少需要2条
    base_score = len(base_triggers)
    if base_score < 2:
        return {
            "rebound_watch_flag": False,
            "rebound_watch_level": "none",
            "rebound_watch_reason": "当前不满足反抽观察基础条件",
        }
    
    # ========== 辅助确认条件 ==========
    confirm_score = 0
    confirm_reasons = []
    
    # 1. 资金未继续恶化（非持续流出）
    if capital and "流出" not in capital:
        confirm_score += 1
        confirm_reasons.append("资金未恶化")
    
    # 2. 量能可控（非放量下跌）
    if not (volume == "强" and anchor_return < 0):
        confirm_score += 1
        confirm_reasons.append("量能可控")
    
    # 3. 相对强弱显著为负（超跌）
    if rs < -0.005:
        confirm_score += 1
        confirm_reasons.append("相对超跌")
    
    # 4. 当日跌幅较大（>2%）
    if anchor_return < -0.02:
        confirm_score += 1
        confirm_reasons.append("单日跌幅较大")
    
    # 5. 近5日处于回撤后（动量非强势）
    if momentum_5d not in ["价强量稳", "价增量涨"]:
        confirm_score += 1
        confirm_reasons.append("处于回撤后")
    
    # ========== 综合判断 ==========
    total_score = base_score + confirm_score
    
    if total_score >= 6:
        level = "high"
        reason = f"多重条件共振：{', '.join(base_triggers[:2])} + {', '.join(confirm_reasons[:3])}"
    elif total_score >= 4:
        level = "medium"
        reason = f"条件匹配：{', '.join(base_triggers[:2])} + {', '.join(confirm_reasons[:2])}"
    elif total_score >= 3:
        level = "low"
        reason = f"初步触发：{', '.join(base_triggers[:2])}"
    else:
        return {
            "rebound_watch_flag": False,
            "rebound_watch_level": "none",
            "rebound_watch_reason": "辅助条件不足，暂不进入观察区",
        }
    
    return {
        "rebound_watch_flag": True,
        "rebound_watch_level": level,
        "rebound_watch_reason": reason,
    }


def format_rebound_section(result: dict) -> str:
    """格式化反抽观察小节"""
    if not result["rebound_watch_flag"]:
        return ""
    
    level_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(result["rebound_watch_level"], "⚪")
    
    lines = [
        "### 🔄 反抽观察",
        "",
        f"{level_emoji} **观察级别：{result['rebound_watch_level'].upper()}**",
        "",
        f"**触发原因：**{result['rebound_watch_reason']}",
        "",
        "**次日重点观察：**",
        "- 早盘是否出现缩量企稳",
        "- 主力资金是否回流",
        "- 板块情绪是否改善",
        "",
        "> ⚠️ **注意**：反抽观察仅表示存在短线反弹可能性，不构成买入建议",
    ]
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="反抽观察层 v1.0")
    parser.add_argument("--date", type=str, help="指定日期 (YYYYMMDD)")
    args = parser.parse_args()
    
    date_str = args.date
    if date_str:
        date_str = pd.to_datetime(date_str).strftime("%Y-%m-%d")
    
    # 加载数据
    data = load_scored_metrics(date_str)
    if data is None:
        print(f"[ERROR] 未找到 {'今日' if not date_str else date_str} 的数据")
        sys.exit(1)
    
    # 计算反抽观察
    result = calc_rebound_watch(data)
    
    # 输出
    print("=" * 60)
    print(f"反抽观察层 v1.0 - {data.get('trade_date', 'N/A')}")
    print("=" * 60)
    print()
    print(f"Flag: {result['rebound_watch_flag']}")
    print(f"Level: {result['rebound_watch_level']}")
    print(f"Reason: {result['rebound_watch_reason']}")
    print()
    
    if result['rebound_watch_flag']:
        print(format_rebound_section(result))


if __name__ == "__main__":
    main()
