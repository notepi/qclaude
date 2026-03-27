"""
报告生成模块
生成 Markdown 格式的每日复盘报告。
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from src.shared.storage import Storage, REPORTS_DIR
from src.shared.config import load_config
from src.dailyreport.data_product import (
    get_price_context,
    get_score_snapshot,
    get_rebound_section,
)

# 存储层
PRICE_STORAGE = Storage("price")


def load_latest_metrics(data_path: str = None) -> Dict[str, Any]:
    """加载最新交易日的指标数据"""
    if data_path is None:
        data_path = PRICE_STORAGE.get_processed_path("daily_metrics.parquet")

    data_file = Path(data_path)
    if not data_file.exists():
        raise FileNotFoundError(f"指标数据文件不存在: {data_file}")

    try:
        df = pd.read_parquet(data_file)
    except Exception as e:
        raise ValueError(f"读取指标数据失败: {e}")

    if df.empty:
        raise ValueError(f"指标数据文件为空: {data_file}")

    df = df.sort_values('trade_date', ascending=False)
    latest = df.iloc[0].to_dict()

    if isinstance(latest['trade_date'], pd.Timestamp):
        latest['trade_date'] = latest['trade_date'].to_pydatetime()

    # 兼容旧版数据（v2.1 字段可能不存在）
    latest.setdefault('return_rank_in_sector', None)
    latest.setdefault('amount_rank_in_sector', None)
    latest.setdefault('sector_total_size', latest.get('sector_total_count', 'N/A'))
    latest.setdefault('price_strength_label', 'N/A')
    latest.setdefault('volume_strength_label', 'N/A')
    latest.setdefault('overall_signal_label', 'N/A')
    latest.setdefault('abnormal_signals', [])

    # abnormal_signals 可能从 parquet 读出为 numpy array，统一转 list
    signals = latest.get('abnormal_signals', [])
    if hasattr(signals, 'tolist'):
        signals = signals.tolist()
    latest['abnormal_signals'] = signals if signals else []

    # v2.7 新增字段：确保 numpy array 转 list
    for key in ['diagnosis_reasons', 'next_watch']:
        val = latest.get(key, [])
        if hasattr(val, 'tolist'):
            val = val.tolist()
        latest[key] = val if val else []

    # signal_breakdown 内的 array 转 list
    signal_breakdown = latest.get('signal_breakdown', {})
    if isinstance(signal_breakdown, dict):
        for k, v in signal_breakdown.items():
            if hasattr(v, 'tolist'):
                signal_breakdown[k] = v.tolist()
        latest['signal_breakdown'] = signal_breakdown

    return latest


# ─────────────────────────────────────────────
# 格式化工具
# ─────────────────────────────────────────────

def fmt_pct(value: Optional[float], with_sign: bool = True) -> str:
    """格式化百分比，正数加 + 号"""
    if value is None:
        return "N/A"
    if with_sign and value > 0:
        return f"+{value:.2%}"
    return f"{value:.2%}"


def fmt_amount(value: Optional[float]) -> str:
    """格式化成交额

    Tushare amount 字段单位为【千元】，需先转换：
      千元 → 万元：/ 10
      千元 → 亿元：/ 100_000

    显示规则：
      >= 1 亿元 → 显示亿元（保留2位小数）
      < 1 亿元  → 显示万元（保留2位小数）
    """
    if value is None:
        return "N/A"
    # Tushare amount 单位：千元 → 转为万元
    wan = value / 10.0
    if wan >= 10_000:
        yi = wan / 10_000
        return f"{yi:.2f} 亿元"
    return f"{wan:,.2f} 万元"


def fmt_multiple(value: Optional[float]) -> str:
    """格式化倍数"""
    if value is None:
        return "N/A"
    return f"{value:.2f}x"


def fmt_rank(rank: Optional[int], total: int) -> str:
    """格式化排名"""
    if rank is None:
        return "N/A"
    return f"第 {rank} / {total}"


def fmt_rs_series(series: list) -> str:
    """格式化相对强弱序列为单行展示（旧→新，最后一项标注今日）"""
    if not series:
        return "N/A"
    parts = []
    for i, v in enumerate(series):
        if v is None:
            parts.append("N/A")
        else:
            sign = "+" if v >= 0 else ""
            s = f"{sign}{v * 100:.1f}%"
            if i == len(series) - 1:
                s += "（今日）"
            parts.append(s)
    return " → ".join(parts)


def fmt_avg_series(series: list) -> str:
    """格式化成交额倍数序列为单行展示（旧→新，最后一项标注今日）"""
    if not series:
        return "N/A"
    parts = []
    for i, v in enumerate(series):
        if v is None:
            parts.append("N/A")
        else:
            s = f"{v:.2f}x"
            if i == len(series) - 1:
                s += "（今日）"
            parts.append(s)
    return " → ".join(parts)


def build_capital_summary(metrics: Dict[str, Any]) -> str:
    """
    v2.5B.1：基于资金结构字段生成一句话解读。
    规则驱动，复盘口吻，不超过一句话。
    """
    net_mf        = metrics.get('net_mf_amount')
    retail_net    = metrics.get('retail_order_net')
    big_ratio     = metrics.get('big_order_ratio')
    struct_label  = metrics.get('capital_structure_label', '')
    relation      = metrics.get('price_capital_relation_label', '')

    # 数据不可用时降级
    if net_mf is None or retail_net is None:
        return ""

    def _flow(v, threshold=200):
        if v > threshold:   return "净流入"
        if v < -threshold:  return "净流出"
        return "基本持平"

    big_flow    = _flow(net_mf, 200)
    retail_flow = _flow(retail_net, 200)
    ratio_str   = f"{big_ratio*100:.0f}%" if big_ratio is not None else "N/A"

    # 按结构标签 + 价格资金关系组合生成解读
    if struct_label == "大资金主导买入" and relation == "上行配合":
        return f"大资金主导买入（占比{ratio_str}），价格与资金同向走强，结构较为健康。"

    if struct_label == "大资金主导卖出" and relation == "下行配合":
        return f"大资金主导卖出（占比{ratio_str}），价格与资金同向走弱，卖压明确。"

    if struct_label == "资金分歧":
        return (f"大资金{big_flow}，中小资金{retail_flow}，"
                f"资金结构分化，方向存在分歧。")

    if struct_label == "中小资金主导":
        if relation == "下跌背离":
            return (f"大资金占比偏低（{ratio_str}），中小资金逆势{retail_flow}，"
                    f"股价下跌但有中小资金承接，短线存在一定支撑。")
        if relation == "上涨背离":
            return (f"大资金占比偏低（{ratio_str}），中小资金推动价格上涨，"
                    f"但大资金{big_flow}，上涨持续性存疑。")
        return (f"大资金参与度偏低（{ratio_str}），以中小资金交易为主，"
                f"大资金{big_flow}，中小资金{retail_flow}。")

    if relation == "上涨背离":
        return (f"股价上涨但大资金净流出（{net_mf:+.0f}万元），"
                f"中小资金{retail_flow}，价格与资金方向背离，需关注持续性。")

    if relation == "下跌背离":
        return (f"股价下跌但大资金净流入（{net_mf:+.0f}万元），"
                f"资金存在一定承接，跌势或有支撑。")

    if relation == "下行配合":
        return (f"大资金净流出（{net_mf:+.0f}万元），中小资金{retail_flow}，"
                f"价格与资金同向走弱。")

    if relation == "上行配合":
        return (f"大资金净流入（{net_mf:+.0f}万元），中小资金{retail_flow}，"
                f"价格与资金同向走强。")

    # 兜底：中性
    return (f"大资金{big_flow}（{net_mf:+.0f}万元），中小资金{retail_flow}，"
            f"资金方向整体中性。")


def build_rolling_section(
    rolling: Optional[Dict[str, Any]],
    anchor_name: str,
    today_amount_vs_5d_avg: Optional[float] = None,
    today_relative_strength: Optional[float] = None,
) -> str:
    """
    生成"近N日状态"章节（v2.5A：加入数字化展示）

    Args:
        rolling: rolling_metrics 数据
        anchor_name: 标的名称
        today_amount_vs_5d_avg: 今天的成交额/5日均值（用于追加到序列末尾）
        today_relative_strength: 今天的相对强弱（用于追加到序列末尾）
    """
    if rolling is None:
        return """## 四、近5日状态

> 连续观察层数据不可用（首次运行或 archive 数据不足）。
"""

    available = rolling.get("available_days", 0)
    if available < 2:
        return f"""## 四、近5日状态

> 历史数据不足（当前仅 {available} 日归档），近5日状态暂不可用。
"""

    n = available
    price_label  = rolling.get("price_trend_label", "N/A")
    volume_label = rolling.get("volume_trend_label", "N/A")
    momentum_label = rolling.get("momentum_label", "N/A")
    trend_summary  = rolling.get("trend_summary", "")

    rs_series  = rolling.get("rs_5d_series", [])
    avg_series = rolling.get("amount_vs_5d_avg_series", [])
    rs_mean = rolling.get("rs_5d_mean")

    # 把今天的值追加到序列末尾（rolling 读取的是历史数据，不包含今天）
    # 然后取最近5天（包含今天）
    if today_relative_strength is not None:
        rs_series = list(rs_series) + [today_relative_strength]
    if today_amount_vs_5d_avg is not None:
        avg_series = list(avg_series) + [today_amount_vs_5d_avg]
    # 取最近5天
    rs_series = rs_series[-5:] if len(rs_series) > 5 else rs_series
    avg_series = avg_series[-5:] if len(avg_series) > 5 else avg_series
    # 更新 n 为实际天数
    n = len(rs_series)

    # 追加今天值后，重新计算连续性统计
    n = len(rs_series)
    rs_out_days = sum(1 for v in rs_series if v is not None and v > 0)
    rs_consec_out = 0
    for v in reversed(rs_series):
        if v is not None and v > 0:
            rs_consec_out += 1
        else:
            break
    rs_consec_und = 0
    for v in reversed(rs_series):
        if v is not None and v <= 0:
            rs_consec_und += 1
        else:
            break

    # 成交额连续性（放量/缩量）
    vol_expand_days = sum(1 for v in avg_series if v is not None and v > 1.0)
    vol_consec_exp = 0
    for v in reversed(avg_series):
        if v is not None and v > 1.0:
            vol_consec_exp += 1
        else:
            break
    vol_consec_shr = 0
    for v in reversed(avg_series):
        if v is not None and v < 1.0:
            vol_consec_shr += 1
        else:
            break

    # 主力资金连续性（保留原有逻辑，因为 daily_metrics 中有今天的资金数据）
    mf_inflow_days    = rolling.get("mf_inflow_days_5d")
    mf_consec_in      = rolling.get("mf_consecutive_inflow", 0) or 0
    mf_consec_out     = rolling.get("mf_consecutive_outflow", 0) or 0
    mf_5d_mean        = rolling.get("mf_5d_mean")
    cf_trend_label    = rolling.get("capital_flow_trend_label", "资金数据不足")
    high_days         = rolling.get("amount_20d_high_days_5d", 0) or 0

    # 资金连续性小块（有数据才显示）
    if mf_inflow_days is not None:
        mf_mean_str = f"{mf_5d_mean:+.0f}万元" if mf_5d_mean is not None else "N/A"
        capital_block = (
            f"\n**主力资金连续性**："
            f"净流入 {mf_inflow_days}/{n} 日 | "
            f"连续净流入 {mf_consec_in} 日 | "
            f"连续净流出 {mf_consec_out} 日 | "
            f"近{n}日均值 {mf_mean_str}"
            f"\n\n**资金趋势**：{cf_trend_label}"
        )
    else:
        capital_block = ""

    rs_mean_str = f"{rs_mean:+.2%}" if rs_mean is not None else "N/A"
    if mf_inflow_days is not None:
        rolling_summary = (
            f"近{n}日跑赢板块 {rs_out_days}/{n} 日，平均相对强弱 {rs_mean_str}；"
            f"主力资金净流入 {mf_inflow_days}/{n} 日，整体呈现 {cf_trend_label}。"
        )
    else:
        rolling_summary = (
            f"近{n}日跑赢板块 {rs_out_days}/{n} 日，平均相对强弱 {rs_mean_str}；"
            f"量能整体维持 {volume_label}。"
        )

    # 动量 emoji
    momentum_emoji_map = {
        "量价齐升": "🟢", "价强量稳": "🟢",
        "价强量弱": "🟡", "短期震荡": "🟡",
        "放量下跌": "🔴", "量价齐弱": "🔴",
    }
    m_emoji = momentum_emoji_map.get(momentum_label, "⚪")

    # 序列展示（旧→新，最后一项标注今日）
    def _rs_seq(series):
        if not series: return "N/A"
        parts = []
        for i, v in enumerate(series):
            s = "N/A" if v is None else f"{'+'if v>=0 else ''}{v*100:.1f}%"
            parts.append(s + "（今日）" if i == len(series)-1 else s)
        return " → ".join(parts)

    def _avg_seq(series):
        if not series: return "N/A"
        parts = []
        for i, v in enumerate(series):
            s = "N/A" if v is None else f"{v:.2f}x"
            parts.append(s + "（今日）" if i == len(series)-1 else s)
        return " → ".join(parts)

    # v2.6: 压缩近5日结构，合并到一行摘要
    # 计算更准确的描述
    if rs_out_days >= 3 and rs_mean is not None and rs_mean > 0:
        structure_desc = "结构偏强"
    elif rs_out_days >= 3 and rs_mean is not None and rs_mean <= 0:
        structure_desc = "胜率尚可，但幅度偏弱"
    elif rs_out_days < 3 and rs_mean is not None and rs_mean > 0:
        structure_desc = "幅度尚可，但胜率偏低"
    else:
        structure_desc = "结构偏弱"

    # 量能描述
    if vol_expand_days >= 3:
        volume_desc = "放量明显"
    elif vol_consec_shr >= 2:
        volume_desc = "连续缩量"
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

    return f"""## 四、近{n}日结构

跑赢板块 **{rs_out_days}/{n} 日**，平均相对强弱 **{rs_mean_str}**，属于"{structure_desc}"；量能 **{volume_desc}**；{capital_desc}。

相对强弱序列：{_rs_seq(rs_series)}
"""


def label_emoji(label: str) -> str:
    """为标签加 emoji"""
    mapping = {
        "强": "🟢 强",
        "中": "🟡 中",
        "弱": "🔴 弱",
        "中性偏强": "🟢 中性偏强",
        "中性": "🟡 中性",
        "中性偏弱": "🔴 中性偏弱",
    }
    return mapping.get(label, label)


# ─────────────────────────────────────────────
# 报告生成
# ─────────────────────────────────────────────

def generate_daily_report(
    metrics_path: str = None,
    config_path: str = None,
) -> str:
    """
    生成每日复盘报告（v2.5 结构）

    固定8部分：
    1. 状态面板
    2. 今日结论
    3. 近5日状态
    4. 核心指标
    5. 板块位置
    6. 今日关注信号
    7. 研究层对比
    8. 股票池快照
    9. 股票池复审提醒
    """
    # 1. 加载数据
    config = load_config(config_path)
    metrics = load_latest_metrics(metrics_path)

    # 获取行情上下文（通过数据产品接口）
    price_context = get_price_context()
    price_product = price_context["price_product"]
    rolling = price_context["rolling"]

    anchor_config = config.get('anchor', {})
    anchor_symbol = anchor_config.get('code')
    anchor_name = anchor_config.get('name')

    if not anchor_symbol:
        raise ValueError("配置文件中缺少 anchor.code")
    if not anchor_name:
        raise ValueError("配置文件中缺少 anchor.name")

    if metrics.get('anchor_symbol') != anchor_symbol:
        raise ValueError(
            f"数据中的 anchor_symbol ({metrics.get('anchor_symbol')}) "
            f"与配置 ({anchor_symbol}) 不一致"
        )

    # 日期格式化
    trade_date = metrics['trade_date']
    if isinstance(trade_date, datetime):
        date_str = trade_date.strftime('%Y-%m-%d')
        date_file_str = trade_date.strftime('%Y%m%d')
    else:
        date_str = str(trade_date)
        date_file_str = date_str.replace('-', '')

    product_trade_date = price_product.get("latest_trade_date")
    if product_trade_date and product_trade_date != date_file_str:
        raise ValueError(
            f"价格数据产品交易日 ({product_trade_date}) 与 metrics ({date_file_str}) 不一致"
        )

    # 取值
    anchor_return = metrics.get('anchor_return')
    sector_avg_return = metrics.get('sector_avg_return')
    relative_strength = metrics.get('relative_strength')
    anchor_amount = metrics.get('anchor_amount')
    amount_20d_high = metrics.get('amount_20d_high')
    amount_vs_5d_avg = metrics.get('amount_vs_5d_avg')
    core_universe_count = metrics.get('core_universe_count', 'N/A')
    sector_total_count = metrics.get('sector_total_count', 'N/A')
    return_rank_in_sector = metrics.get('return_rank_in_sector')
    amount_rank_in_sector = metrics.get('amount_rank_in_sector')
    sector_total_size = metrics.get('sector_total_size', 'N/A')
    price_strength_label = metrics.get('price_strength_label', 'N/A')
    volume_strength_label = metrics.get('volume_strength_label', 'N/A')
    overall_signal_label = metrics.get('overall_signal_label', 'N/A')
    abnormal_signals = metrics.get('abnormal_signals', [])
    if hasattr(abnormal_signals, "tolist"):
        abnormal_signals = abnormal_signals.tolist()
    abnormal_signals = list(abnormal_signals) if abnormal_signals else []

    # v2.5 新增状态标签
    valuation_label    = metrics.get('valuation_label', 'N/A')
    capital_flow_label = metrics.get('capital_flow_label', 'N/A')
    activity_label     = metrics.get('activity_label', 'N/A')

    # v3.1 新增多维度估值
    valuation_detail   = metrics.get('valuation_detail', '')
    sector_pe_mean     = metrics.get('sector_pe_mean')
    sector_pb_mean     = metrics.get('sector_pb_mean')
    sector_ps_mean     = metrics.get('sector_ps_mean')
    pe_vs_sector       = metrics.get('pe_vs_sector')
    pb_vs_sector       = metrics.get('pb_vs_sector')
    ps_vs_sector       = metrics.get('ps_vs_sector')
    pe_sector_position = metrics.get('pe_sector_position', 'N/A')
    pb_sector_position = metrics.get('pb_sector_position', 'N/A')
    ps_sector_position = metrics.get('ps_sector_position', 'N/A')
    pe_percentile_60d  = metrics.get('pe_percentile_60d')
    pb_percentile_60d  = metrics.get('pb_percentile_60d')
    ps_percentile_60d  = metrics.get('ps_percentile_60d')
    pe_percentile_label = metrics.get('pe_percentile_label', 'N/A')
    pb_percentile_label = metrics.get('pb_percentile_label', 'N/A')
    ps_percentile_label = metrics.get('ps_percentile_label', 'N/A')

    # v2.5B 新增资金结构标签
    capital_structure_label      = metrics.get('capital_structure_label', 'N/A')
    price_capital_relation_label = metrics.get('price_capital_relation_label', 'N/A')
    retail_order_net             = metrics.get('retail_order_net')
    big_order_ratio              = metrics.get('big_order_ratio')
    net_mf_amount                = metrics.get('net_mf_amount')

    # v2.6 新增 research_core 字段
    research_avg_return        = metrics.get('research_avg_return')
    research_relative_strength = metrics.get('research_relative_strength')
    price_data_product_status  = metrics.get('price_data_product_status') or price_product.get('overall_status', 'N/A')
    market_data_status         = metrics.get('market_data_status') or price_product.get('market_data_status', 'N/A')
    daily_basic_status         = metrics.get('daily_basic_status') or price_product.get('daily_basic_status', 'N/A')
    moneyflow_status           = metrics.get('moneyflow_status') or price_product.get('moneyflow_status', 'N/A')

    # rolling 动量标签（用于状态面板）
    momentum_label = rolling.get('momentum_label', 'N/A') if rolling else 'N/A'

    anchor_display = f"{anchor_name}（{anchor_symbol}）"

    # ─────────────────────────────────────────
    # 第0部分：状态面板（v2.5）
    # ─────────────────────────────────────────

    def _panel_emoji(label: str) -> str:
        """为状态面板标签加 emoji"""
        green  = {"强", "中性偏强", "量价齐升", "价强量稳", "主力偏多", "活跃", "低估值", "中性估值",
                  "大资金主导买入", "上行配合", "下跌背离"}
        red    = {"弱", "中性偏弱", "量价齐弱", "放量下跌", "主力偏空", "低活跃", "高估值",
                  "大资金主导卖出", "上涨背离", "下行配合"}
        yellow = {"中", "中性", "短期震荡", "主力中性", "正常", "中性偏高",
                  "价强量弱", "短期相对强势", "短期中性", "短期走弱",
                  "连续强于板块", "连续弱于板块",
                  "中小资金主导", "资金分歧", "方向不明"}
        if label in green:  return f"🟢 {label}"
        if label in red:    return f"🔴 {label}"
        if label in yellow: return f"🟡 {label}"
        return label

    section_panel = f"""## 📊 状态面板

| 维度 | 状态 |
|------|------|
| 价格强度 | {_panel_emoji(price_strength_label)} |
| 成交额强度 | {_panel_emoji(volume_strength_label)} |
| 大资金方向 | {_panel_emoji(capital_flow_label)} |
| 资金结构 | {_panel_emoji(capital_structure_label)} |
| 价格资金关系 | {_panel_emoji(price_capital_relation_label)} |
| 活跃度 | {_panel_emoji(activity_label)} |
| 估值 | {_panel_emoji(valuation_label)} |
| 近5日结构 | {_panel_emoji(momentum_label)} |
"""

    # ─────────────────────────────────────────
    # 第1部分：今日结论（v2.7 从数据线读取）
    # ─────────────────────────────────────────

    # 从数据线读取诊断字段
    diagnosis_label = metrics.get("diagnosis_label", "状态待确认")
    diagnosis_reasons = metrics.get("diagnosis_reasons", ["数据不足"])
    next_watch_list = metrics.get("next_watch", ["量能与资金流向变化"])

    # 格式化
    current_state = diagnosis_label
    primary_reasons = "、".join(diagnosis_reasons[:3]) if diagnosis_reasons else "跟随板块波动"
    next_watch = "、".join(next_watch_list[:3])

    section_conclusion = f"""## 一、今日结论

- **当前状态**：{current_state}
- **主要原因**：{primary_reasons}
- **下一步观察**：{next_watch}
"""

    # 保留研究层分化提示（压缩到一行）
    research_note = ""
    if research_relative_strength is not None and relative_strength is not None:
        rs_diff = research_relative_strength - relative_strength
        if abs(rs_diff) > 0.005:
            if rs_diff > 0:
                research_note = f"研究层较交易层强 {rs_diff:.2%}，产业链对比占优"
            else:
                research_note = f"研究层较交易层弱 {abs(rs_diff):.2%}，铂力特在核心环节表现更优"

    if research_note:
        section_conclusion = section_conclusion.rstrip() + f"\n\n> {research_note}，但短线定价仍主要受交易层和资金面驱动。\n"

    # ─────────────────────────────────────────
    # 第2部分：近5日状态（v2.7 从数据线读取）
    # ─────────────────────────────────────────
    rolling_summary_text = metrics.get("rolling_summary_text", "数据不足")
    rs_5d_series = rolling.get("rs_5d_series", []) if rolling else []

    # 追加今日值到序列末尾
    if rs_5d_series and relative_strength is not None:
        rs_5d_series = list(rs_5d_series) + [relative_strength]
    rs_5d_series = rs_5d_series[-5:] if len(rs_5d_series) > 5 else rs_5d_series

    # 格式化序列
    def _fmt_rs_series(series):
        parts = []
        for v in series:
            if v is None:
                parts.append("N/A")
            else:
                sign = "+" if v >= 0 else ""
                parts.append(f"{sign}{v * 100:.1f}%")
        return " → ".join(parts)

    rs_series_str = _fmt_rs_series(rs_5d_series)
    if rs_5d_series:
        rs_series_str += "（今日）"

    section_rolling = f"""## 四、近5日结构

{rolling_summary_text}

相对强弱序列：{rs_series_str}
"""

    # ─────────────────────────────────────────
    # 第3部分：核心指标
    # ─────────────────────────────────────────
    amount_20d_str = "✅ 是" if amount_20d_high else "否"

    # v2.4 新增字段
    pe_ttm        = metrics.get('pe_ttm')
    pb            = metrics.get('pb')
    total_mv      = metrics.get('total_mv')
    turnover_rate = metrics.get('turnover_rate')
    buy_elg_vol   = metrics.get('buy_elg_vol')
    sell_elg_vol  = metrics.get('sell_elg_vol')

    def fmt_mv(v):
        """总市值：万元 → 亿元"""
        if v is None: return "N/A"
        return f"{v / 10000:.1f} 亿元"

    def fmt_flow(v):
        """资金净流入：万元，带符号"""
        if v is None: return "N/A"
        sign = "+" if v >= 0 else ""
        if abs(v) >= 10000:
            return f"{sign}{v/10000:.2f} 亿元"
        return f"{sign}{v:.0f} 万元"

    def fmt_vol(v):
        if v is None: return "N/A"
        return f"{v:.0f} 手"

    # v2.6: 精简核心指标（6个关键指标）
    section_metrics = f"""## 二、证据板

| 指标 | 数值 |
|------|------|
| 涨跌幅 | {fmt_pct(anchor_return)} |
| 板块均值 | {fmt_pct(sector_avg_return)} |
| 相对强弱 | {fmt_pct(relative_strength)} |
| 成交额/5日均值 | {fmt_multiple(amount_vs_5d_avg)} |
| 主力净流入 | {fmt_flow(net_mf_amount)} |
| 换手率 | {turnover_rate:.2f}% |
"""

    # ─────────────────────────────────────────
    # 资金流向（v2.7 从数据线读取）
    # ─────────────────────────────────────────
    capital_summary_text = metrics.get("capital_summary_text", "数据缺失")
    elg_action_text = metrics.get("elg_action_text", "数据缺失")
    participation_text = metrics.get("participation_text", "数据缺失")
    capital_conclusion_text = metrics.get("capital_conclusion_text", "数据缺失")

    section_capital_flow = f"""## 三、资金流向

- **主力总体**：{capital_summary_text}
- **超大单行为**：{elg_action_text}
- **大资金参与度**：{participation_text}
- **结论**：{capital_conclusion_text}
"""

    # ─────────────────────────────────────────
    # 第5部分：估值与位置（v2.6 合并）
    # ─────────────────────────────────────────
    # 估值简化描述
    if valuation_detail:
        # 将"估值偏低"改为更准确的说法
        valuation_desc = valuation_detail.replace("估值偏低", "相对估值低位")
        # 加上"绝对估值仍不算低"的提示
        if "低位" in valuation_desc and "绝对" not in valuation_desc:
            valuation_desc += "，绝对估值仍不算低"
    else:
        valuation_desc = "估值数据缺失"

    # 板块位置
    rank_total = sector_total_size if isinstance(sector_total_size, int) else "N/A"
    def fmt_rank_natural(rank: Optional[int], total: int) -> str:
        if rank is None or not isinstance(total, int):
            return "N/A"
        if rank == 1:
            return f"第1/{total}名"
        if rank == total:
            return f"第{total}/{total}名（末位）"
        return f"第{rank}/{total}名"

    return_rank_str = fmt_rank_natural(return_rank_in_sector, rank_total) if isinstance(rank_total, int) else "N/A"
    amount_rank_str = fmt_rank_natural(amount_rank_in_sector, rank_total) if isinstance(rank_total, int) else "N/A"

    section_valuation_position = f"""## 五、估值与位置

**估值**：{valuation_desc}
**板块位置**：涨跌幅 {return_rank_str} | 成交额 {amount_rank_str}
"""

    # ─────────────────────────────────────────
    # 第6部分：今日关键信号（v2.7 从数据线读取）
    # ─────────────────────────────────────────
    signal_breakdown = metrics.get("signal_breakdown", {})
    abnormal_list = signal_breakdown.get("abnormal", [])
    positive_list = signal_breakdown.get("positive", [])
    risk_list = signal_breakdown.get("risk", [])

    section_signals = f"""## 六、今日关键信号

- **异常**：{"、".join(abnormal_list) if abnormal_list else "无"}
- **积极**：{"、".join(positive_list) if positive_list else "无"}
- **风险**：{"、".join(risk_list) if risk_list else "无"}
"""

    # ─────────────────────────────────────────
    # 第7部分：明日观察清单（v2.7 从数据线读取）
    # ─────────────────────────────────────────
    next_watch_list = metrics.get("next_watch", ["量能与资金流向变化"])
    watch_md = "\n".join(f"- {item}" for item in next_watch_list[:4])

    section_watchlist = f"""## 七、明日观察清单

{watch_md if watch_md else "- 无特殊观察项"}
"""

    # ─────────────────────────────────────────
    # 第八节：研究层对比（v2.6 简化）
    # ─────────────────────────────────────────
    if research_avg_return is not None and research_relative_strength is not None:
        section_research = f"""## 八、研究层对比

| 视角 | 板块均值 | 相对强弱 |
|:---|:---:|:---:|
| 交易层 | {fmt_pct(sector_avg_return)} | {fmt_pct(relative_strength)} |
| 研究层 | {fmt_pct(research_avg_return)} | {fmt_pct(research_relative_strength)} |

> 注：研究层对比说明产业链中期相对位次，但短线定价仍主要受交易层和资金面驱动。
"""
    else:
        section_research = ""

    # ─────────────────────────────────────────
    # 第九节：股票池快照（精简模式）
    # ─────────────────────────────────────────
    # 读取配置中的股票池分层
    core_universe = config.get('core_universe', [])
    
    # 整理各层股票列表
    def format_stock_list(stocks, max_show=5):
        if not stocks:
            return "无"
        names = [s.get('name', s.get('code', '')) for s in stocks]
        if len(names) <= max_show:
            return " / ".join(names)
        return f"{' / '.join(names[:max_show])}...（共{len(names)}只）"
    
    changelog = config.get("changelog", [])
    latest_change_date = changelog[0].get("date") if changelog else None
    report_trade_date = date_str

    if latest_change_date and latest_change_date == report_trade_date:
        section_pool = f"""## 九、股票池快照

- **核心层**：{format_stock_list(core_universe)}

> 其余层级维持既有结构。
"""
    else:
        section_pool = """## 九、股票池快照

> 股票池结构未发生变化。
"""

    # ─────────────────────────────────────────
    # 第九节：股票池复审提醒（v2.8 新增）
    # ─────────────────────────────────────────
    # 导入并运行复审分析
    try:
        from src.dailyreport.review_stock_pool import analyze_pool, generate_summary
        review_results = analyze_pool(config, trade_date if isinstance(trade_date, datetime) else datetime.now())
        review_summary = generate_summary(review_results)
        
        # 构建复审提醒小节
        review_lines = []
        
        if review_summary["due_list"]:
            review_lines.append("**需立即复审**：")
            for item in review_summary["due_list"][:3]:  # 最多显示3条
                review_lines.append(f"- {item['name']} ({item['pool_role']}) - 已过期 {abs(item['days_to_review'])} 天")
        
        if review_summary["upcoming_list"]:
            if review_lines:
                review_lines.append("")
            review_lines.append("**即将到期**：")
            for item in review_summary["upcoming_list"][:3]:
                review_lines.append(f"- {item['name']} ({item['pool_role']}) - {item['days_to_review']} 天后")
        
        if not review_lines:
            review_content = "> 暂无需要复审的标的"
        else:
            review_content = "\n".join(review_lines)

        section_review = f"""## 十、股票池复审提醒

{review_content}

> 共 {review_summary['total_stocks']} 只标的，{review_summary['due_count']} 只需复审，{review_summary['upcoming_count']} 只即将到期
"""
    except Exception as e:
        section_review = f"""## 十、股票池复审提醒

> 复审模块加载失败: {e}
"""

    # ─────────────────────────────────────────
    # 组装完整报告
    # ─────────────────────────────────────────
    universe_version = metrics.get('universe_version', 'unknown')

    md_content = f"""# {anchor_name}每日复盘 - {date_str}

{section_panel}
{section_conclusion}
{section_metrics}
{section_capital_flow}
{section_rolling}
{section_valuation_position}
{section_signals}
{section_watchlist}
{section_research}
{section_pool}
{section_review}
---

*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | v2.7 | 股票池版本: {universe_version}*
"""

    # 保存报告
    try:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise ValueError(f"创建报告目录失败: {e}")

    report_filename = f"{date_file_str}_blt_review.md"
    report_path = REPORTS_DIR / report_filename

    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(md_content)
    except Exception as e:
        raise ValueError(f"写入报告文件失败: {e}")

    # 控制台摘要
    print("=" * 55)
    print(f"  {anchor_name}每日复盘 - {date_str}  [v2.7]")
    print("=" * 55)
    print(f"  综合信号  : {overall_signal_label}")
    print(f"  涨跌幅    : {fmt_pct(anchor_return)}  (板块: {fmt_pct(sector_avg_return)}, 相对: {fmt_pct(relative_strength)})")
    print(f"  成交额    : {fmt_amount(anchor_amount)}  ({fmt_multiple(amount_vs_5d_avg)} vs 5日均)")
    print(f"  板块位置  : 涨跌幅 {fmt_rank_natural(return_rank_in_sector, rank_total) if isinstance(rank_total, int) else 'N/A'}  |  成交额 {fmt_rank_natural(amount_rank_in_sector, rank_total) if isinstance(rank_total, int) else 'N/A'}")
    if abnormal_signals:
        print(f"  关注信号  : {' | '.join(abnormal_signals)}")
    else:
        print(f"  关注信号  : 无")
    print("-" * 55)
    print(f"  报告已保存: {report_path}")
    print("=" * 55)

    return str(report_path)


def main():
    """主函数"""
    report_path = generate_daily_report()
    return report_path


if __name__ == "__main__":
    main()
