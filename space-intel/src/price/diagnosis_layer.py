"""
诊断层模块 v1.1
基于 daily_metrics 和 rolling_metrics 产出诊断标签、原因、观察清单

职责：
  - 综合诊断标签（diagnosis_label）
  - 诊断原因列表（diagnosis_reasons）
  - 明日观察清单（next_watch）
  - 信号拆解（signal_breakdown: abnormal/positive/risk）
  - 近5日摘要文本（rolling_summary_text，含今日数据）

不做：
  - 原始数据计算
  - 复杂统计检验
"""

from typing import Dict, Any, List, Optional


def compute_rolling_summary_with_today(
    rolling: Optional[Dict[str, Any]],
    today_relative_strength: Optional[float],
    today_amount_vs_5d_avg: Optional[float],
) -> str:
    """
    计算包含今日数据的近5日摘要文本。

    Args:
        rolling: rolling_metrics 数据
        today_relative_strength: 今日相对强弱
        today_amount_vs_5d_avg: 今日成交额/5日均值

    Returns:
        近5日摘要文本
    """
    if rolling is None:
        return "数据不足"

    # 获取历史数据
    rs_series = rolling.get("rs_5d_series", [])
    avg_series = rolling.get("amount_vs_5d_avg_series", [])
    mf_inflow_days = rolling.get("mf_inflow_days_5d")
    capital_flow_trend_label = rolling.get("capital_flow_trend_label", "")

    # 追加今日数据
    if today_relative_strength is not None:
        rs_series = list(rs_series) + [today_relative_strength]
    if today_amount_vs_5d_avg is not None:
        avg_series = list(avg_series) + [today_amount_vs_5d_avg]

    # 取最近5天
    rs_series = rs_series[-5:] if len(rs_series) > 5 else rs_series
    avg_series = avg_series[-5:] if len(avg_series) > 5 else avg_series
    n = len(rs_series)

    if n < 2:
        return f"历史数据不足（当前仅 {n} 日）"

    # 计算跑赢天数
    out_days = sum(1 for v in rs_series if v is not None and v > 0)

    # 计算平均相对强弱
    valid_rs = [v for v in rs_series if v is not None]
    rs_mean = sum(valid_rs) / len(valid_rs) if valid_rs else 0
    rs_mean_str = f"{rs_mean * 100:+.2f}%"

    # 结构描述
    if out_days >= 4:
        structure_desc = "胜率较高"
    elif out_days >= 3:
        if rs_mean > 0:
            structure_desc = "胜率尚可，幅度占优"
        else:
            structure_desc = "胜率尚可，但幅度偏弱"
    elif out_days >= 2:
        structure_desc = "结构中性"
    else:
        structure_desc = "胜率偏低"

    # 量能描述
    consec_shrink = 0
    for v in reversed(avg_series):
        if v is not None and v < 0.8:
            consec_shrink += 1
        else:
            break

    consec_expand = 0
    for v in reversed(avg_series):
        if v is not None and v > 1.2:
            consec_expand += 1
        else:
            break

    if consec_shrink >= 2:
        volume_desc = "连续缩量"
    elif consec_expand >= 2:
        volume_desc = "连续放量"
    else:
        volume_desc = "量能平稳"

    # 资金描述
    if mf_inflow_days is not None:
        if mf_inflow_days >= 3:
            capital_desc = "资金面偏强"
        elif mf_inflow_days >= 2:
            capital_desc = "资金面有改善迹象"
        else:
            capital_desc = "资金面偏弱"
    else:
        capital_desc = "资金数据缺失"

    return f"跑赢板块 **{out_days}/{n} 日**，平均相对强弱 **{rs_mean_str}**，属于\"{structure_desc}\"；量能 **{volume_desc}**；{capital_desc}。"


def compute_diagnosis(
    metrics: Dict[str, Any],
    rolling: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    基于指标数据计算诊断结果。

    Args:
        metrics: daily_metrics 数据（来自 analyzer.py）
        rolling: rolling_metrics 数据（来自 rolling_analyzer.py）

    Returns:
        {
            "diagnosis_label": "中性偏强",
            "diagnosis_reasons": ["近5日结构未坏", "主力未确认回流"],
            "next_watch": ["主力是否转正", "成交额放大"],
            "signal_breakdown": {
                "abnormal": [],
                "positive": ["近5日结构未坏"],
                "risk": ["主力资金流出"]
            }
        }
    """
    # 提取关键字段
    relative_strength = metrics.get("relative_strength")
    anchor_return = metrics.get("anchor_return")
    sector_avg_return = metrics.get("sector_avg_return")
    net_mf_amount = metrics.get("net_mf_amount")
    amount_vs_5d_avg = metrics.get("amount_vs_5d_avg")
    capital_flow_label = metrics.get("capital_flow_label", "")
    overall_signal_label = metrics.get("overall_signal_label", "中性")
    price_strength_label = metrics.get("price_strength_label", "")
    volume_strength_label = metrics.get("volume_strength_label", "")
    research_relative_strength = metrics.get("research_relative_strength")

    # rolling 指标
    momentum_label = rolling.get("momentum_label", "") if rolling else ""
    capital_flow_trend_label = rolling.get("capital_flow_trend_label", "") if rolling else ""

    # ─────────────────────────────────────────
    # 1. 诊断标签（基于 overall_signal_label 微调）
    # ─────────────────────────────────────────
    diagnosis_label = overall_signal_label

    # 如果有主力流出但价格尚稳，降一级
    if net_mf_amount is not None and net_mf_amount < -1000:
        if "偏强" in diagnosis_label:
            diagnosis_label = diagnosis_label.replace("偏强", "")
            if diagnosis_label in ["强", "中性"]:
                pass
            else:
                diagnosis_label = "中性"
        elif diagnosis_label == "中性":
            diagnosis_label = "中性"

    # 加上状态描述
    if "偏强" in diagnosis_label or diagnosis_label == "强":
        state_desc = f"{diagnosis_label}，但尚未走出独立上攻"
    elif "偏弱" in diagnosis_label or diagnosis_label == "弱":
        state_desc = f"{diagnosis_label}，下行压力仍存"
    else:
        state_desc = f"{diagnosis_label}，暂未形成明确方向"

    # ─────────────────────────────────────────
    # 2. 诊断原因
    # ─────────────────────────────────────────
    reasons = []

    # 价格层面
    if relative_strength is not None and relative_strength > 0.005:
        reasons.append("跑赢板块")
    elif relative_strength is not None and relative_strength < -0.005:
        reasons.append("跑输板块")

    # 近5日结构
    if momentum_label in ["价强量稳", "量价齐升"]:
        reasons.append("近5日结构未坏")
    elif momentum_label in ["量价齐弱", "放量下跌"]:
        reasons.append("近5日结构转弱")

    # 资金层面
    if net_mf_amount is not None and net_mf_amount > 1000:
        reasons.append("主力资金流入")
    elif net_mf_amount is not None and net_mf_amount < -1000:
        reasons.append("主力未确认回流")

    # 成交额
    if amount_vs_5d_avg is not None and amount_vs_5d_avg > 1.2:
        reasons.append("成交额放大")
    elif amount_vs_5d_avg is not None and amount_vs_5d_avg < 0.8:
        reasons.append("成交额萎缩")

    # 默认原因
    if not reasons:
        reasons.append("跟随板块波动，暂未见独立驱动")

    # ─────────────────────────────────────────
    # 3. 明日观察清单
    # ─────────────────────────────────────────
    watch_items = []

    if net_mf_amount is not None and net_mf_amount < 0:
        watch_items.append("主力是否转正")

    if amount_vs_5d_avg is not None and amount_vs_5d_avg < 1.0:
        watch_items.append("成交额能否放大至5日均值以上")

    if "流出" in capital_flow_trend_label:
        watch_items.append("资金面是否转暖")

    # 研究层对比
    if research_relative_strength is not None and relative_strength is not None:
        if research_relative_strength - relative_strength > 0.005:
            watch_items.append("研究层对标是否继续走弱")

    if relative_strength is not None and relative_strength > 0:
        watch_items.append("是否继续跑赢交易层板块")

    if not watch_items:
        watch_items.append("量能与资金流向变化")

    # ─────────────────────────────────────────
    # 4. 信号拆解
    # ─────────────────────────────────────────
    abnormal = []
    positive = []
    risk = []

    # 异常信号（来自 abnormal_signals，排除排名类）
    abnormal_signals = metrics.get("abnormal_signals", [])
    if hasattr(abnormal_signals, "tolist"):
        abnormal_signals = abnormal_signals.tolist()
    for sig in abnormal_signals:
        if sig not in {"涨幅位居板块前二", "成交额位居板块前二"}:
            abnormal.append(sig)

    # 积极信号
    if relative_strength is not None and relative_strength > 0.005:
        positive.append("跑赢板块")
    if momentum_label in ["价强量稳", "量价齐升"]:
        positive.append("近5日结构未坏")
    if research_relative_strength is not None and relative_strength is not None:
        if research_relative_strength - relative_strength > 0.005:
            positive.append("研究层相对更强")

    # 风险信号
    if net_mf_amount is not None and net_mf_amount < -1000:
        risk.append("主力资金流出")
    if "空" in capital_flow_label:
        risk.append("主力未确认回流")
    if amount_vs_5d_avg is not None and amount_vs_5d_avg < 0.9:
        risk.append("成交额萎缩")

    return {
        "diagnosis_label": state_desc,
        "diagnosis_reasons": reasons[:3],  # 最多3条
        "next_watch": watch_items[:4],     # 最多4条
        "signal_breakdown": {
            "abnormal": abnormal,
            "positive": positive,
            "risk": risk
        }
    }


def compute_capital_text(metrics: Dict[str, Any]) -> Dict[str, str]:
    """
    计算资金流向的文本描述。

    Args:
        metrics: daily_metrics 数据

    Returns:
        {
            "capital_summary_text": "净流出 2882 万元，整体偏空",
            "elg_action_text": "净买入 3236 手，盘中存在局部承接",
            "participation_text": "25.3%，参与度偏低，未形成主导",
            "capital_conclusion_text": "有承接但力度不足，不足以扭转弱势"
        }
    """
    net_mf_amount = metrics.get("net_mf_amount")
    buy_elg_vol = metrics.get("buy_elg_vol")
    sell_elg_vol = metrics.get("sell_elg_vol")
    big_order_ratio = metrics.get("big_order_ratio")

    # 超大单净量
    net_elg_vol = None
    if buy_elg_vol is not None and sell_elg_vol is not None:
        net_elg_vol = buy_elg_vol - sell_elg_vol

    # 1. 主力总体
    if net_mf_amount is not None:
        if net_mf_amount > 1000:
            capital_summary_text = f"净流入 {net_mf_amount:.0f} 万元，整体偏多"
        elif net_mf_amount < -1000:
            capital_summary_text = f"净流出 {abs(net_mf_amount):.0f} 万元，整体偏空"
        else:
            capital_summary_text = f"净流入 {net_mf_amount:.0f} 万元，整体中性"
    else:
        capital_summary_text = "数据缺失"

    # 2. 超大单行为
    if net_elg_vol is not None:
        if net_elg_vol > 0:
            elg_action_text = f"净买入 {net_elg_vol:.0f} 手，盘中存在局部承接"
        elif net_elg_vol < 0:
            elg_action_text = f"净卖出 {abs(net_elg_vol):.0f} 手，高规格资金离场"
        else:
            elg_action_text = "基本持平"
    else:
        elg_action_text = "数据缺失"

    # 3. 大资金参与度
    if big_order_ratio is not None:
        ratio_pct = big_order_ratio * 100
        if ratio_pct < 30:
            participation_text = f"{ratio_pct:.1f}%，参与度偏低，未形成主导"
        elif ratio_pct > 50:
            participation_text = f"{ratio_pct:.1f}%，参与度较高，主力主导明显"
        else:
            participation_text = f"{ratio_pct:.1f}%，参与度适中"
    else:
        participation_text = "数据缺失"

    # 4. 综合结论
    if net_mf_amount is not None and net_mf_amount < 0:
        if net_elg_vol is not None and net_elg_vol > 0:
            capital_conclusion_text = "有承接但力度不足，不足以扭转弱势"
        else:
            capital_conclusion_text = "资金面偏弱，无明显承接迹象"
    elif net_mf_amount is not None and net_mf_amount > 0:
        capital_conclusion_text = "资金面偏强，主力有做多意愿"
    else:
        capital_conclusion_text = "资金面中性"

    return {
        "capital_summary_text": capital_summary_text,
        "elg_action_text": elg_action_text,
        "participation_text": participation_text,
        "capital_conclusion_text": capital_conclusion_text
    }