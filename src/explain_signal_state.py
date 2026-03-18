"""
交易解释层 v2.0
基于现有评分和趋势，输出"状态->原因->下一步观察点"

用法：
    python3 src/explain_signal_state.py
    python3 src/explain_signal_state.py --date 20260317

输入：
    - signal_score, signal_rating, signal_breakdown
    - overall_signal_label, price_strength_label, volume_strength_label
    - momentum_label, capital_flow_trend_label
    - relative_strength, research_relative_strength
    - 近5日趋势字段
    - 事件层摘要

输出：
    - state_summary: 一句话总结当前状态
    - driver_summary: 2-3条核心驱动因素
    - next_watchpoint: 明天最该关注的1个变量
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


def explain_state(data: dict) -> dict:
    """
    生成状态解释
    
    Returns:
        {
            "state_summary": str,
            "driver_summary": list,
            "next_watchpoint": str,
            "rebound_watch_flag": bool,
        }
    """
    score = data.get("signal_score", 0)
    rating = data.get("signal_rating", "中性")
    breakdown = data.get("signal_breakdown", "")
    
    overall = data.get("overall_signal_label", "")
    price = data.get("price_strength_label", "")
    volume = data.get("volume_strength_label", "")
    momentum = data.get("momentum_label", "")
    capital = data.get("capital_flow_trend_label", "")
    
    rs = data.get("relative_strength", 0) or 0
    research_rs = data.get("research_relative_strength", 0) or 0
    
    # 近5日趋势
    price_trend_5d = data.get("price_trend_label", "")
    volume_trend_5d = data.get("volume_trend_label", "")
    momentum_5d = data.get("momentum_label", "")  # 复用
    
    # 事件层
    abnormal_signals = data.get("abnormal_signals", [])
    
    # ========== 1. state_summary ==========
    state_summary = ""
    
    if rating == "偏强":
        state_summary = "当前处于相对强势状态，但需警惕持续性"
    elif rating == "中性偏强":
        state_summary = "整体偏强，但部分维度存在分歧"
    elif rating == "中性":
        state_summary = "当前状态中性，方向尚不明确"
    elif rating == "中性偏弱":
        state_summary = "整体偏弱，但尚未出现恐慌性抛售"
    elif rating == "偏弱":
        state_summary = "当前处于弱势状态，关注是否超跌"
    else:
        state_summary = "当前状态待观察"
    
    # ========== 2. driver_summary ==========
    drivers = []
    
    # 价格维度
    if price == "弱":
        if rs < -0.01:
            drivers.append(f"价格走弱，跑输板块 {rs:.2%}")
        else:
            drivers.append("价格走弱，但相对板块尚可")
    elif price == "强":
        if rs > 0.01:
            drivers.append(f"价格强势，跑赢板块 {rs:.2%}")
        else:
            drivers.append("价格相对强势")
    
    # 资金维度
    if capital and "流出" in capital:
        drivers.append("主力资金持续流出")
    elif capital and "流入" in capital:
        drivers.append("主力资金流入")
    
    # 量能维度
    if volume == "强" and price == "弱":
        drivers.append("放量下跌，抛压较重")
    elif volume == "弱" and price == "弱":
        drivers.append("缩量下跌，抛压减轻")
    
    # 动量维度
    if momentum == "价强量稳":
        drivers.append("量价配合良好，动量健康")
    elif momentum == "量价齐弱":
        drivers.append("量价齐弱，动量不足")
    
    # 研究层分化
    if abs(research_rs - rs) > 0.005:
        if research_rs > rs:
            drivers.append(f"研究层相对更强（差 {research_rs - rs:.2%}），关注华曙高科/中天火箭走势")
        else:
            drivers.append(f"研究层相对更弱（差 {rs - research_rs:.2%}），铂力特在核心环节占优")
    
    # 异常信号
    if abnormal_signals and len(abnormal_signals) > 0:
        drivers.append(f"异常信号: {', '.join(abnormal_signals[:2])}")
    
    # 限制2-3条
    driver_summary = drivers[:3]
    if not driver_summary:
        driver_summary = ["暂无显著驱动因素"]
    
    # ========== 3. next_watchpoint ==========
    next_watch = ""
    rebound_watch = False
    
    # 优先级1: 资金
    if capital and "流出" in capital:
        next_watch = "明日主力资金是否回流"
        rebound_watch = True
    # 优先级2: 价格
    elif price == "弱" and volume == "弱":
        next_watch = "是否出现缩量企稳信号"
        rebound_watch = True
    # 优先级3: 板块
    elif rs < -0.01:
        next_watch = "板块情绪是否改善"
    # 优先级4: 研究层
    elif abs(research_rs - rs) > 0.005:
        next_watch = "研究层与交易层分化是否收敛"
    # 默认
    else:
        next_watch = "成交量与资金流向变化"
    
    # ========== 4. rebound_watch_flag ==========
    # 超跌反弹观察标志
    rebound_conditions = [
        1 if rating in ["偏弱", "中性偏弱"] else 0,
        1 if price == "弱" else 0,
        1 if (volume == "弱" or (capital and "流出" in capital)) else 0,
    ]
    rebound_watch = sum(rebound_conditions) >= 2
    
    return {
        "state_summary": state_summary,
        "driver_summary": driver_summary,
        "next_watchpoint": next_watch,
        "rebound_watch_flag": rebound_watch,
    }


def format_explanation(result: dict, data: dict = None) -> str:
    """格式化输出"""
    lines = [
        "### 📊 状态解释",
        "",
        f"**{result['state_summary']}**",
        "",
        "**核心驱动：**",
    ]
    
    for driver in result["driver_summary"]:
        lines.append(f"- {driver}")
    
    lines.extend([
        "",
        f"**下一步关注：**{result['next_watchpoint']}",
    ])
    
    if result.get("rebound_watch_flag"):
        lines.append("")
        lines.append("⚠️ **超跌反弹观察**：当前处于弱势，但抛压减轻，关注反弹信号")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="交易解释层 v2.0")
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
    
    # 生成解释
    result = explain_state(data)
    
    # 输出
    print("=" * 60)
    print(f"交易解释层 v2.0 - {data.get('trade_date', 'N/A')}")
    print("=" * 60)
    print()
    print(format_explanation(result, data))
    print()
    print(f"评分: {data.get('signal_score', 0)} ({data.get('signal_rating', 'N/A')})")
    print(f"分解: {data.get('signal_breakdown', 'N/A')}")


if __name__ == "__main__":
    main()
