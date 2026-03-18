"""
报告生成模块
生成 Markdown 格式的每日复盘报告。
"""

import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"
DATA_DIR = PROJECT_ROOT / "data" / "processed"
CONFIG_DIR = PROJECT_ROOT / "config"


def load_config(config_path: str = None) -> Dict[str, Any]:
    """加载配置文件"""
    if config_path is None:
        config_path = CONFIG_DIR / "stocks.yaml"

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_file}")

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"配置文件格式错误: {e}")


def load_latest_metrics(data_path: str = None) -> Dict[str, Any]:
    """加载最新交易日的指标数据"""
    if data_path is None:
        data_path = DATA_DIR / "daily_metrics.parquet"

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


def build_rolling_section(rolling: Optional[Dict[str, Any]], anchor_name: str) -> str:
    """
    生成"近N日状态"章节（v2.5A：加入数字化展示）
    """
    if rolling is None:
        return """## 二、近5日状态

> 连续观察层数据不可用（首次运行或 archive 数据不足）。
"""

    available = rolling.get("available_days", 0)
    if available < 2:
        return f"""## 二、近5日状态

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

    # 连续性数字
    rs_out_days   = rolling.get("rs_outperform_days_5d", 0) or 0
    rs_consec_out = rolling.get("rs_consecutive_outperform", 0) or 0
    rs_consec_und = rolling.get("rs_consecutive_underperform", 0) or 0

    vol_expand_days  = rolling.get("volume_expand_days_5d", 0) or 0
    vol_consec_exp   = rolling.get("volume_consecutive_expand", 0) or 0
    vol_consec_shr   = rolling.get("volume_consecutive_shrink", 0) or 0
    high_days        = rolling.get("amount_20d_high_days_5d", 0) or 0

    # v2.5C P1：主力资金连续性
    mf_inflow_days    = rolling.get("mf_inflow_days_5d")
    mf_consec_in      = rolling.get("mf_consecutive_inflow", 0) or 0
    mf_consec_out     = rolling.get("mf_consecutive_outflow", 0) or 0
    mf_5d_mean        = rolling.get("mf_5d_mean")
    cf_trend_label    = rolling.get("capital_flow_trend_label", "资金数据不足")

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

    return f"""## 二、近{n}日状态

| 维度 | 标签 |
|------|------|
| 价格趋势 | {price_label} |
| 量能趋势 | {volume_label} |
| **综合动量** | **{m_emoji} {momentum_label}** |

**价格连续性**：跑赢板块 {rs_out_days}/{n} 日 | 连续跑赢 {rs_consec_out} 日 | 连续跑输 {rs_consec_und} 日

**量能连续性**：放量 {vol_expand_days}/{n} 日 | 连续放量 {vol_consec_exp} 日 | 连续缩量 {vol_consec_shr} 日 | 创20日新高 {high_days} 次
{capital_block}

**相对强弱序列**：{_rs_seq(rs_series)}

**成交额倍数序列**：{_avg_seq(avg_series)}

> {trend_summary}

> 摘要：{rolling_summary}
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
    events_path: str = None,
    include_events: bool = True,
) -> str:
    """
    生成每日复盘报告（v2.2 结构）

    固定5部分：
    1. 今日结论
    2. 核心指标
    3. 板块位置
    4. 今日关注信号
    5. 今日可能驱动因素（事件层，可选）
    """
    # 1. 加载数据
    config = load_config(config_path)
    metrics = load_latest_metrics(metrics_path)
    try:
        from price_data_product import load_price_data_product
        price_product = load_price_data_product()
    except Exception as e:
        raise ValueError(f"价格数据产品不可用: {e}")

    # 加载事件层（可选，不存在时降级）
    events = None
    if include_events:
        try:
            from event_layer import load_events
            events = load_events(events_path)
        except Exception:
            pass  # 事件层不可用时静默降级

    # 加载连续观察层（可选，不存在时降级）
    rolling = None
    try:
        from rolling_analyzer import load_rolling_metrics
        rolling = load_rolling_metrics()
    except Exception:
        pass  # 连续观察层不可用时静默降级

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

    # v2.5B 新增资金结构标签
    capital_structure_label      = metrics.get('capital_structure_label', 'N/A')
    price_capital_relation_label = metrics.get('price_capital_relation_label', 'N/A')
    retail_order_net             = metrics.get('retail_order_net')
    big_order_ratio              = metrics.get('big_order_ratio')

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
    # 第1部分：今日结论 + 诊断层 v1
    # ─────────────────────────────────────────
    
    # === 诊断层 v1 ===
    diagnosis_reasons = []  # 主诊断原因
    consistency_note = ""   # 一致性判断
    next_watch = ""        # 下一步关注
    
    # 1. 主诊断：弱/强 的原因（最多2条）
    # 板块整体判断
    sector_weak = sector_avg_return is not None and sector_avg_return < -0.02  # 板块跌超2%
    stock_weak = relative_strength is not None and relative_strength < 0  # 跑输板块
    stock_strong = relative_strength is not None and relative_strength > 0  # 跑赢板块
    
    # 资金判断
    capital_weak = capital_flow_label is not None and ("偏空" in capital_flow_label or "流出" in capital_flow_label)
    capital_strong = capital_flow_label is not None and ("偏多" in capital_flow_label or "流入" in capital_flow_label)
    
    # 放量下跌判断
    volume_up = volume_strength_label == "强"
    price_down = anchor_return is not None and anchor_return < 0
    volume_price_weak = volume_up and price_down
    
    # 生成主诊断
    if overall_signal_label and "弱" in overall_signal_label:
        if sector_weak and stock_weak:
            diagnosis_reasons.append("板块整体下跌，个股跌幅更大")
        elif sector_weak:
            diagnosis_reasons.append("板块整体回调")
        elif stock_weak:
            diagnosis_reasons.append("个股走势弱于板块")
        
        if capital_weak:
            diagnosis_reasons.append("主力资金净流出")
        
        if volume_price_weak:
            diagnosis_reasons.append("放量下跌，短线承压")
    
    elif overall_signal_label and "强" in overall_signal_label:
        if sector_weak and stock_strong:
            diagnosis_reasons.append("板块下跌中逆势走强")
        elif stock_strong:
            diagnosis_reasons.append("走势强于板块")
        
        if capital_strong:
            diagnosis_reasons.append("主力资金净流入")
    
    # 限制输出1-2条
    diagnosis_reasons = diagnosis_reasons[:2]
    
    # 2. 一致性判断
    momentum = rolling.get('momentum_label', '') if rolling else ''
    if rolling:
        rs_5d = rolling.get('rs_5d_mean', 0) or 0
        
        # 今日弱 vs 近5日强
        if overall_signal_label and "弱" in overall_signal_label:
            if momentum in ["量价齐升", "价强量稳"] or rs_5d > 0.01:
                consistency_note = "短线回撤，但近5日仍维持强势结构"
            elif momentum in ["量价齐弱", "放量下跌"]:
                consistency_note = "跌势延续，弱势结构确认"
            else:
                consistency_note = "与近5日趋势基本一致"
        
        # 今日强 vs 近5日弱
        elif overall_signal_label and "强" in overall_signal_label:
            if momentum in ["量价齐弱", "放量下跌"] or rs_5d < -0.01:
                consistency_note = "单日反弹，需观察持续性"
            elif momentum in ["量价齐升", "价强量稳"]:
                consistency_note = "强势延续，趋势向好"
            else:
                consistency_note = "与近5日趋势基本一致"
    
    # 3. 下一步关注（只输出1条）
    if capital_weak:
        next_watch = "明日主力资金是否回流"
    elif sector_weak:
        next_watch = "板块情绪是否企稳"
    elif momentum and "弱" in momentum:
        next_watch = "近5日趋势是否改善"
    elif rolling and rolling.get('capital_flow_trend_label'):
        if "流出" in rolling['capital_flow_trend_label']:
            next_watch = "资金面是否转暖"
    
    # 当前状态
    if overall_signal_label == "强":
        current_state = "整体偏强，价格与量能维持较好配合。"
    elif overall_signal_label == "中性偏强":
        current_state = "整体偏强，但仍需继续确认持续性。"
    elif overall_signal_label == "中性":
        current_state = "整体中性，暂未形成明确方向。"
    elif overall_signal_label == "中性偏弱":
        current_state = "整体偏弱，短线仍处于回撤与消化阶段。"
    elif overall_signal_label == "弱":
        current_state = "整体偏弱，价格与资金面仍有压力。"
    else:
        current_state = "当前状态待进一步确认。"

    primary_reasons = "；".join(diagnosis_reasons) if diagnosis_reasons else "主要表现为跟随板块波动，暂未见独立驱动。"
    next_watch = next_watch or "成交量与资金流向变化"

    section_conclusion = f"""## 一、今日结论

- **当前状态**：{current_state}
- **主要原因**：{primary_reasons}
- **下一步观察**：{next_watch}
"""

    # ─────────────────────────────────────────
    # 交易解释层 v2.0 增强（内联简化版）
    # ─────────────────────────────────────────
    
    # 生成状态解释（只做证据拆解，不重复结论）
    explain_lines = []

    drivers = []
    
    # 价格维度
    if price_strength_label == "弱":
        if relative_strength is not None and relative_strength < -0.01:
            drivers.append(f"价格走弱，跑输板块 {relative_strength:.2%}")
        else:
            drivers.append("价格走弱，但相对板块尚可")
    elif price_strength_label == "强":
        if relative_strength is not None and relative_strength > 0.01:
            drivers.append(f"价格强势，跑赢板块 {relative_strength:.2%}")
        else:
            drivers.append("价格相对强势")
    
    # 资金维度
    cf_trend = rolling.get('capital_flow_trend_label', '') if rolling else ''
    if cf_trend and "流出" in cf_trend:
        drivers.append("主力资金持续流出")
    elif cf_trend and "流入" in cf_trend:
        drivers.append("主力资金流入")
    
    # 量能维度
    if volume_strength_label == "强" and price_strength_label == "弱":
        drivers.append("放量下跌，抛压较重")
    elif volume_strength_label == "弱" and price_strength_label == "弱":
        drivers.append("缩量下跌，抛压减轻")
    
    # 动量维度
    if momentum_label == "价强量稳":
        if overall_signal_label in {"中性偏弱", "弱"}:
            drivers.append("短期回撤，但中期结构未明显失真")
        else:
            drivers.append("近5日结构尚稳，量价关系未见明显破坏")
    elif momentum_label == "量价齐升":
        if overall_signal_label in {"中性偏弱", "弱"}:
            drivers.append("近5日结构尚未完全破坏")
        else:
            drivers.append("近5日结构偏强，量价仍有配合")
    elif momentum_label == "量价齐弱":
        drivers.append("量价齐弱，动量不足")
    elif momentum_label == "放量下跌":
        drivers.append("放量回撤，短线承压仍较明显")
    
    # 研究层分化
    if research_relative_strength is not None and relative_strength is not None:
        if abs(research_relative_strength - relative_strength) > 0.005:
            if research_relative_strength > relative_strength:
                drivers.append(f"研究层相对更强（差 {research_relative_strength - relative_strength:.2%}），关注华曙高科/中天火箭走势")
            else:
                drivers.append(f"研究层相对更弱（差 {relative_strength - research_relative_strength:.2%}），铂力特在核心环节占优")
    
    if not drivers:
        drivers.append("暂无额外证据补充")
    
    explain_lines.append("### 状态解释")
    for driver in drivers[:4]:
        explain_lines.append(f"- {driver}")

    explanation_str = "\n".join(explain_lines)
    section_conclusion = section_conclusion.rstrip() + f"\n\n{explanation_str}\n"

    # ─────────────────────────────────────────
    # 评分层快照（翻译成人话）
    score_snapshot = ""
    try:
        from score_layer import calc_score

        score_row = pd.Series({
            "overall_signal_label": overall_signal_label,
            "price_strength_label": price_strength_label,
            "volume_strength_label": volume_strength_label,
            "momentum_label": momentum_label,
            "capital_flow_trend_label": rolling.get("capital_flow_trend_label", "") if rolling else "",
        })
        score_result = calc_score(score_row)

        drag_items = []
        support_items = []

        if price_strength_label == "弱":
            drag_items.append("价格表现偏弱")
        if capital_flow_label == "主力偏空":
            drag_items.append("主力资金偏空")
        if rolling and "流出" in str(rolling.get("capital_flow_trend_label", "")):
            drag_items.append("近5日资金持续流出")
        if volume_strength_label == "弱":
            drag_items.append("量能偏弱")

        if momentum_label in {"价强量稳", "量价齐升"}:
            support_items.append("近5日结构尚未明显破坏")
        if relative_strength is not None and relative_strength > -0.005:
            support_items.append("相对板块并未明显失真")
        if return_rank_in_sector is not None and return_rank_in_sector <= 2:
            support_items.append("板块内位置仍处前列")

        drag_text = "；".join(drag_items[:2]) if drag_items else "暂无明显拖累项"
        support_text = "；".join(support_items[:2]) if support_items else "暂未见明显缓冲项"

        score_snapshot = (
            f"\n\n### 评分层快照\n"
            f"- **整体判断**：{score_result['signal_rating']}\n"
            f"- **主要拖累项**：{drag_text}\n"
            f"- **主要缓冲项**：{support_text}\n"
        )
    except Exception:
        score_snapshot = ""

    section_conclusion = section_conclusion.rstrip() + score_snapshot

    # 反抽观察层 v1.0
    # ─────────────────────────────────────────
    try:
        # 构建输入数据
        rebound_data = {
            "signal_score": 0,
            "signal_rating": "",
            "overall_signal_label": overall_signal_label,
            "price_strength_label": price_strength_label,
            "volume_strength_label": volume_strength_label,
            "capital_flow_trend_label": rolling.get("capital_flow_trend_label", "") if rolling else "",
            "relative_strength": relative_strength,
            "anchor_return": anchor_return,
            "momentum_label": momentum_label,
            "price_trend_label": rolling.get("price_trend_label", "") if rolling else "",
        }
        
        # 计算评分用于确定 rating
        from score_layer import calc_score
        score_result = calc_score(pd.Series(rebound_data))
        rebound_data.update(score_result)
        
        # 计算反抽观察
        from rebound_watch_layer import calc_rebound_watch, format_rebound_section
        rebound_result = calc_rebound_watch(rebound_data)
        
        if rebound_result["rebound_watch_flag"]:
            rebound_section = format_rebound_section(rebound_result)
            section_conclusion = section_conclusion.rstrip() + f"\n\n{rebound_section}\n"
        else:
            section_conclusion = section_conclusion.rstrip() + "\n\n当前未触发反抽观察。\n"
    except Exception:
        section_conclusion = section_conclusion.rstrip() + "\n\n当前未触发反抽观察。\n"

    # ─────────────────────────────────────────
    # 第2部分：近5日状态（连续观察层）
    # ─────────────────────────────────────────
    section_rolling = build_rolling_section(rolling, anchor_name)

    # ─────────────────────────────────────────
    # 第3部分：核心指标
    # ─────────────────────────────────────────
    amount_20d_str = "✅ 是" if amount_20d_high else "否"

    # v2.4 新增字段
    pe_ttm        = metrics.get('pe_ttm')
    pb            = metrics.get('pb')
    total_mv      = metrics.get('total_mv')
    turnover_rate = metrics.get('turnover_rate')
    net_mf_amount = metrics.get('net_mf_amount')
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

    # 基本面行（有数据才显示）
    basic_rows = ""
    if pe_ttm is not None:
        basic_rows += f"| PE_TTM | {pe_ttm:.1f} |\n"
    if pb is not None:
        basic_rows += f"| PB | {pb:.2f} |\n"
    if total_mv is not None:
        basic_rows += f"| 总市值 | {fmt_mv(total_mv)} |\n"
    if turnover_rate is not None:
        basic_rows += f"| 换手率 | {turnover_rate:.2f}% |\n"

    # 资金结构行（有数据才显示）
    flow_rows = ""
    if net_mf_amount is not None:
        flow_rows += f"| 主力净流入 | {fmt_flow(net_mf_amount)} |\n"
    if buy_elg_vol is not None and sell_elg_vol is not None:
        net_elg = buy_elg_vol - sell_elg_vol
        flow_rows += f"| 超大单净量 | {fmt_vol(net_elg)} |\n"
    if retail_order_net is not None:
        flow_rows += f"| 中小资金净流入 | {fmt_flow(retail_order_net)} |\n"
    if big_order_ratio is not None:
        flow_rows += f"| 大资金占比 | {big_order_ratio*100:.1f}% |\n"

    capital_summary = build_capital_summary(metrics)
    capital_summary_md = f"\n> 💡 {capital_summary}" if capital_summary else ""
    section_metrics = f"""## 三、核心指标

| 指标 | 数值 |
|------|------|
| {anchor_name} 涨跌幅 | {fmt_pct(anchor_return)} |
| 板块平均涨跌幅 | {fmt_pct(sector_avg_return)} |
| 相对强弱 | {fmt_pct(relative_strength)} |
| 成交额 | {fmt_amount(anchor_amount)} |
| 成交额创20日新高 | {amount_20d_str} |
| 成交额 / 5日均值 | {fmt_multiple(amount_vs_5d_avg)} |
{basic_rows}{flow_rows}
{capital_summary_md}
"""

    # ─────────────────────────────────────────
    # 第4部分：板块位置
    # ─────────────────────────────────────────
    rank_total = sector_total_size if isinstance(sector_total_size, int) else "N/A"

    def fmt_rank_natural(rank: Optional[int], total: int) -> str:
        if rank is None or not isinstance(total, int):
            return "N/A"
        if rank == 1:
            return f"{total}只样本中居首位"
        if rank == total:
            return f"{total}只样本中居末位"
        mid = (total + 1) / 2
        if rank < mid:
            return f"{total}只样本中位列中间偏前位置（第{rank}位）"
        if rank > mid:
            return f"{total}只样本中位列中间偏后位置（第{rank}位）"
        return f"{total}只样本中位列中间位置（第{rank}位）"

    section_sector = f"""## 四、板块位置

| 维度 | 排名 |
|------|------|
| 涨跌幅位置 | {fmt_rank_natural(return_rank_in_sector, rank_total) if isinstance(rank_total, int) else "N/A"} |
| 成交额位置 | {fmt_rank_natural(amount_rank_in_sector, rank_total) if isinstance(rank_total, int) else "N/A"} |
"""

    # ─────────────────────────────────────────
    # 第5部分：今日关注信号
    # ─────────────────────────────────────────
    # 合并行情信号 + 资金结构背离信号
    all_signals = list(abnormal_signals)

    # 价格资金背离信号（v2.5B.1）
    divergence_signals = {"上涨背离", "下跌背离"}
    if price_capital_relation_label in divergence_signals:
        all_signals.append(f"价格资金背离：{price_capital_relation_label}")

    benign_rank_signals = {"涨幅位居板块前二", "成交额位居板块前二"}
    has_material_signal = any(sig not in benign_rank_signals for sig in all_signals)

    if all_signals and has_material_signal:
        signals_md = "\n".join(f"- ⚡ {sig}" for sig in all_signals)
    else:
        signals_md = "暂无显著异常，主要表现为跟随板块系统性回调。"

    section_signals = f"""## 五、今日关注信号

{signals_md}
"""

    # ─────────────────────────────────────────
    # 第5部分：今日可能驱动因素（事件层）
    # ─────────────────────────────────────────
    if events is not None:
        events_trade_date = str(events.get('trade_date') or '')
        if events_trade_date != date_file_str:
            events = None

    if events is not None:
        announcements = events.get('company_announcements', [])
        company_news = events.get('company_news', [])
        sector_news = events.get('sector_news', [])
        event_signal = events.get('event_signal_label', 'N/A')
        event_summary = events.get('event_summary', '')
        ann_status = events.get('announcement_status', 'unavailable')
        co_news_status = events.get('company_news_status', 'error')
        sec_news_status = events.get('sector_news_status', 'error')

        # ── 公司层文案 ──
        if announcements:
            ann_lines = "\n".join(
                f"- 📋 [{item['title']}]({item['url']})" if item.get('url') else f"- 📋 {item['title']}"
                for item in announcements
            )
            company_layer = f"**公司层**\n\n{ann_lines}"
        elif company_news:
            co_lines = "\n".join(
                f"- {item['title']}（{item.get('source','')} {item.get('datetime','')}）"
                for item in company_news
            )
            company_layer = f"**公司层**\n\n{co_lines}"
        elif ann_status == 'ok' or ann_status == 'empty':
            company_layer = "**公司层**：未见明确公司层新增催化。"
        else:
            company_layer = f"**公司层**：今日未获取到有效公司层信息（公告={ann_status}，新闻={co_news_status}）。"

        # ── 板块新闻文案 ──
        if sector_news:
            sec_lines = "\n".join(
                f"- {item['title']}（{item.get('source','')} {item.get('datetime','')}）"
                for item in sector_news
            )
            sector_layer = f"**板块层**\n\n{sec_lines}"
        elif sec_news_status in ('ok', 'empty'):
            sector_layer = "**板块层**：未见明确板块层新增催化。"
        else:
            sector_layer = f"**板块层**：今日未获取到有效板块层信息（{sec_news_status}）。"

        if event_signal == "有明确催化":
            event_conclusion = "今日存在一定事件配合，但价格表现仍以盘面验证为准。"
        elif event_signal == "有弱催化":
            event_conclusion = "今日存在一定事件扰动，但整体仍更多由板块联动与资金面主导。"
        else:
            event_conclusion = "今日更多由板块联动与资金面主导，未见明确事件催化。"

        section_events = f"""## 六、今日可能驱动因素

{company_layer}

{sector_layer}

> 总结：{event_conclusion}
"""
    else:
        # 事件层不可用（未运行或失败）
        section_events = """## 六、今日可能驱动因素

**公司层**：今日未获取到有效公司层信息。

**板块层**：今日未获取到有效板块层信息。

> 总结：今日更多由板块联动与资金面主导。
"""

    # ─────────────────────────────────────────
    # 第七节：研究层对比（v2.6 新增）
    # ─────────────────────────────────────────
    if research_avg_return is not None and research_relative_strength is not None:
        # 判断差异方向
        rs_diff = research_relative_strength - relative_strength
        if abs(rs_diff) < 0.001:
            diff_note = "两个视角结论一致，说明铂力特在交易层和研究层的相对表现同步。"
        elif rs_diff < -0.005:
            diff_note = f"研究层视角下铂力特更弱（差 {abs(rs_diff):.2%}），通常意味着华曙高科/中天火箭等研究对标近期走势更强，需关注是否存在独立催化（如设备订单/发射计划）导致分化。"
        elif rs_diff > 0.005:
            diff_note = f"研究层视角下铂力特更强（差 {rs_diff:.2%}），说明研究对标近期跑输，铂力特在产业链核心环节的表现相对占优。"
        else:
            diff_note = "两个视角略有差异，但方向一致，属正常波动范围。"

        section_research = f"""## 七、研究层对比

| 视角 | 板块均值 | 相对强弱 |
|:---|:---:|:---:|
| trading_core（交易层） | {fmt_pct(sector_avg_return)} | {fmt_pct(relative_strength)} |
| research_core（研究层） | {fmt_pct(research_avg_return)} | {fmt_pct(research_relative_strength)} |

> **解读**：{diff_note}
"""
    else:
        section_research = """## 七、研究层对比

> 研究层数据暂不可用（请检查 stocks.yaml 是否配置 research_core 或运行完整 pipeline）。
"""

    # ─────────────────────────────────────────
    # 第八节：股票池快照（精简模式）
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
        section_pool = f"""## 八、股票池快照

- **核心层**：{format_stock_list(core_universe)}

> 其余层级维持既有结构。
"""
    else:
        section_pool = """## 八、股票池快照

> 股票池结构未发生变化。
"""

    # ─────────────────────────────────────────
    # 第九节：股票池复审提醒（v2.8 新增）
    # ─────────────────────────────────────────
    # 导入并运行复审分析
    try:
        from review_stock_pool import analyze_pool, generate_summary
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
        
        section_review = f"""## 九、股票池复审提醒

{review_content}

> 共 {review_summary['total_stocks']} 只标的，{review_summary['due_count']} 只需复审，{review_summary['upcoming_count']} 只即将到期
"""
    except Exception as e:
        section_review = f"""## 九、股票池复审提醒

> 复审模块加载失败: {e}
"""

    # ─────────────────────────────────────────
    # 组装完整报告
    # ─────────────────────────────────────────
    universe_version = metrics.get('universe_version', 'unknown')

    md_content = f"""# {anchor_name}每日复盘 - {date_str}

{section_panel}
{section_conclusion}
{section_rolling}
{section_metrics}
{section_sector}
{section_signals}
{section_events}
{section_research}
{section_pool}
{section_review}
---

*报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | v2.8 | 股票池版本: {universe_version}*
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
    print(f"  {anchor_name}每日复盘 - {date_str}  [v2.2]")
    print("=" * 55)
    print(f"  综合信号  : {overall_signal_label}")
    print(f"  涨跌幅    : {fmt_pct(anchor_return)}  (板块: {fmt_pct(sector_avg_return)}, 相对: {fmt_pct(relative_strength)})")
    print(f"  成交额    : {fmt_amount(anchor_amount)}  ({fmt_multiple(amount_vs_5d_avg)} vs 5日均)")
    print(f"  板块位置  : 涨跌幅 {fmt_rank_natural(return_rank_in_sector, rank_total) if isinstance(rank_total, int) else 'N/A'}  |  成交额 {fmt_rank_natural(amount_rank_in_sector, rank_total) if isinstance(rank_total, int) else 'N/A'}")
    if abnormal_signals:
        print(f"  关注信号  : {' | '.join(abnormal_signals)}")
    else:
        print(f"  关注信号  : 无")
    if events is not None:
        print(f"  事件信号  : {events.get('event_signal_label', 'N/A')}")
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
