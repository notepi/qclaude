"""
报告生成模块 v2.1.1
生成结构化 Markdown 格式的每日复盘报告

v2.1 报告结构（固定4部分）：
1. 今日结论
2. 核心指标
3. 板块位置
4. 今日异常信号

配置说明：
- 板块均值仅基于 core_universe 计算
- anchor_symbol（锚定标的）不参与均值计算
- extended_universe 暂不参与均值计算
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
"""
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
    events_path: str = None
) -> str:
    """
    生成每日复盘报告（v2.2 结构）

    固定5部分：
    1. 今日结论
    2. 核心指标
    3. 板块位置
    4. 今日异常信号
    5. 今日可能驱动因素（事件层，可选）
    """
    # 1. 加载数据
    config = load_config(config_path)
    metrics = load_latest_metrics(metrics_path)

    # 加载事件层（可选，不存在时降级）
    events = None
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
    abnormal_signals: List[str] = metrics.get('abnormal_signals', [])

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
| 短期动量 | {_panel_emoji(momentum_label)} |
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
    if rolling:
        momentum = rolling.get('momentum_label', '')
        rs_5d = rolling.get('rs_5d_mean', 0) or 0
        
        # 今日弱 vs 近5日强
        if overall_signal_label and "弱" in overall_signal_label:
            if momentum in ["价强量稳", "价增量涨"] or rs_5d > 0.01:
                consistency_note = "短线回撤，但近5日仍维持强势结构"
            elif momentum in ["价弱量缩", "价跌量缩"]:
                consistency_note = "跌势延续，弱势结构确认"
            else:
                consistency_note = "与近5日趋势基本一致"
        
        # 今日强 vs 近5日弱
        elif overall_signal_label and "强" in overall_signal_label:
            if momentum in ["价弱量缩", "价跌量缩"] or rs_5d < -0.01:
                consistency_note = "单日反弹，需观察持续性"
            elif momentum in ["价强量稳", "价增量涨"]:
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
    
    # 构建诊断字符串
    diagnosis_str = ""
    if diagnosis_reasons:
        diagnosis_str = "**" + "；".join(diagnosis_reasons) + "**"
    
    if consistency_note:
        if diagnosis_str:
            diagnosis_str += f"\n\n- **与近5日状态**：{consistency_note}"
        else:
            diagnosis_str = f"**与近5日状态**：{consistency_note}"
    
    if next_watch:
        if diagnosis_str:
            diagnosis_str += f"\n\n- **下一步关注**：{next_watch}"
        else:
            diagnosis_str = f"**下一步关注**：{next_watch}"
    
    # 拼装结论小节
    section_conclusion = f"""## 一、今日结论

| 维度 | 标签 |
|------|------|
| 价格强度 | {label_emoji(price_strength_label)} |
| 成交额强度 | {label_emoji(volume_strength_label)} |
| **综合信号** | **{label_emoji(overall_signal_label)}** |

### 📌 诊断
{diagnosis_str}
"""

    # ─────────────────────────────────────────
    # 交易解释层 v2.0 增强（内联简化版）
    # ─────────────────────────────────────────
    
    # 生成状态解释
    explain_lines = []
    
    # 1. 状态总结
    if overall_signal_label:
        if "强" in overall_signal_label and "偏弱" not in overall_signal_label:
            state_summary = "当前处于相对强势状态，但需警惕持续性"
        elif "偏弱" in overall_signal_label:
            state_summary = "整体偏弱，但尚未出现恐慌性抛售"
        elif "弱" in overall_signal_label:
            state_summary = "当前处于弱势状态，关注是否超跌"
        else:
            state_summary = "当前状态中性，方向尚不明确"
    else:
        state_summary = "当前状态待观察"
    
    explain_lines.append(f"**{state_summary}**")
    explain_lines.append("")
    
    # 2. 核心驱动
    explain_lines.append("**核心驱动：**")
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
        drivers.append("量价配合良好，动量健康")
    elif momentum_label == "量价齐弱":
        drivers.append("量价齐弱，动量不足")
    
    # 研究层分化
    if research_relative_strength is not None and relative_strength is not None:
        if abs(research_relative_strength - relative_strength) > 0.005:
            if research_relative_strength > relative_strength:
                drivers.append(f"研究层相对更强（差 {research_relative_strength - relative_strength:.2%}），关注华曙高科/中天火箭走势")
            else:
                drivers.append(f"研究层相对更弱（差 {relative_strength - research_relative_strength:.2%}），铂力特在核心环节占优")
    
    # 异常信号
    if abnormal_signals and len(abnormal_signals) > 0:
        drivers.append(f"异常信号: {', '.join(abnormal_signals[:2])}")
    
    if not drivers:
        drivers.append("暂无显著驱动因素")
    
    for driver in drivers[:3]:
        explain_lines.append(f"- {driver}")
    
    explain_lines.append("")
    
    # 3. 下一步关注
    next_watch = ""
    if cf_trend and "流出" in cf_trend:
        next_watch = "明日主力资金是否回流"
    elif price_strength_label == "弱" and volume_strength_label == "弱":
        next_watch = "是否出现缩量企稳信号"
    elif relative_strength is not None and relative_strength < -0.01:
        next_watch = "板块情绪是否改善"
    elif research_relative_strength is not None and relative_strength is not None and abs(research_relative_strength - relative_strength) > 0.005:
        next_watch = "研究层与交易层分化是否收敛"
    else:
        next_watch = "成交量与资金流向变化"
    
    explain_lines.append(f"**下一步关注：**{next_watch}")
    
    # 4. 超跌反弹观察
    rebound_conditions = [
        1 if overall_signal_label and ("偏弱" in overall_signal_label or "弱" in overall_signal_label) else 0,
        1 if price_strength_label == "弱" else 0,
        1 if (volume_strength_label == "弱" or (cf_trend and "流出" in cf_trend)) else 0,
    ]
    if sum(rebound_conditions) >= 2:
        explain_lines.append("")
        explain_lines.append("⚠️ **超跌反弹观察**：当前处于弱势，但抛压减轻，关注反弹信号")
    
    explanation_str = "\n".join(explain_lines)
    section_conclusion = section_conclusion.rstrip() + f"\n\n### 📊 状态解释\n\n{explanation_str}\n"

    # ─────────────────────────────────────────
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
    except Exception:
        pass

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
> **均值口径**：板块均值基于 core_universe（{core_universe_count}/{sector_total_count} 只），**不含{anchor_name}**。
{capital_summary_md}
"""

    # ─────────────────────────────────────────
    # 第4部分：板块位置
    # ─────────────────────────────────────────
    rank_total = sector_total_size if isinstance(sector_total_size, int) else "N/A"

    section_sector = f"""## 四、板块位置

> **排名口径**：core_universe（{core_universe_count} 只）+ {anchor_name}，共 {rank_total} 只，**不含 extended_universe**。  
> 与均值口径的区别：排名将{anchor_name}纳入比较，均值计算则将其排除。

| 维度 | 排名 |
|------|------|
| 涨跌幅排名 | {fmt_rank(return_rank_in_sector, rank_total) if isinstance(rank_total, int) else "N/A"} |
| 成交额排名 | {fmt_rank(amount_rank_in_sector, rank_total) if isinstance(rank_total, int) else "N/A"} |
"""

    # ─────────────────────────────────────────
    # 第5部分：今日异常信号
    # ─────────────────────────────────────────
    # 合并行情异常信号 + 资金结构背离信号
    all_signals = list(abnormal_signals)

    # 价格资金背离信号（v2.5B.1）
    divergence_signals = {"上涨背离", "下跌背离"}
    if price_capital_relation_label in divergence_signals:
        all_signals.append(f"价格资金背离：{price_capital_relation_label}")

    if all_signals:
        signals_md = "\n".join(f"- ⚡ {sig}" for sig in all_signals)
    else:
        signals_md = "今日无显著异常信号。"

    section_signals = f"""## 五、今日异常信号

{signals_md}
"""

    # ─────────────────────────────────────────
    # 第5部分：今日可能驱动因素（事件层）
    # ─────────────────────────────────────────
    if events is not None:
        announcements = events.get('company_announcements', [])
        company_news = events.get('company_news', [])
        sector_news = events.get('sector_news', [])
        event_signal = events.get('event_signal_label', 'N/A')
        event_summary = events.get('event_summary', '')
        ann_status = events.get('announcement_status', 'unavailable')
        co_news_status = events.get('company_news_status', 'error')
        sec_news_status = events.get('sector_news_status', 'error')

        # ── 公告文案（按状态分支，严格区分"没拿到"和"确认没有"）──
        if announcements:
            ann_lines = "\n".join(
                f"- 📋 [{item['title']}]({item['url']})" if item.get('url') else f"- 📋 {item['title']}"
                for item in announcements
            )
            ann_block = f"**公司公告**\n\n{ann_lines}"
        elif ann_status == 'ok' or ann_status == 'empty':
            ann_block = "**公司公告**：今日无公告"
        elif ann_status == 'unavailable':
            ann_block = "**公司公告**：公告源暂不可用（v2.2.2 补充）"
        elif ann_status == 'timeout':
            ann_block = "**公司公告**：今日未获取到有效公告信息（请求超时）"
        elif ann_status == 'permission_denied':
            ann_block = "**公司公告**：今日未获取到有效公告信息（权限不足）"
        else:
            ann_block = "**公司公告**：今日未获取到有效公告信息"

        # ── 公司新闻文案 ──
        if company_news:
            co_lines = "\n".join(
                f"- {item['title']}（{item.get('source','')} {item.get('datetime','')}）"
                for item in company_news
            )
            co_block = f"**公司新闻**\n\n{co_lines}"
        elif co_news_status in ('ok', 'empty'):
            co_block = "**公司新闻**：今日无相关新闻"
        else:
            co_block = f"**公司新闻**：今日未获取到有效信息（{co_news_status}）"

        # ── 板块新闻文案 ──
        if sector_news:
            sec_lines = "\n".join(
                f"- {item['title']}（{item.get('source','')} {item.get('datetime','')}）"
                for item in sector_news
            )
            sec_block = f"**板块新闻**\n\n{sec_lines}"
        elif sec_news_status in ('ok', 'empty'):
            sec_block = "**板块新闻**：今日无相关板块新闻"
        else:
            sec_block = f"**板块新闻**：今日未获取到有效信息（{sec_news_status}）"

        # ── 兜底文案（三类均无实质内容且状态可信时）──
        all_confirmed_empty = (
            not announcements and ann_status in ('ok', 'empty') and
            not company_news and co_news_status in ('ok', 'empty') and
            not sector_news and sec_news_status in ('ok', 'empty')
        )
        no_event_note = "\n\n> 今日未发现明确公司或板块事件催化。" if all_confirmed_empty else ""

        section_events = f"""## 六、今日可能驱动因素

> 事件信号：**{event_signal}**  
> {event_summary}

{ann_block}

{co_block}

{sec_block}{no_event_note}
"""
    else:
        # 事件层不可用（未运行或失败）
        section_events = """## 六、今日可能驱动因素

> 事件层数据不可用（请运行 pipeline 完整流程或检查 daily_events.json）。
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
    # 第八节：股票池快照（v2.7 新增）
    # ─────────────────────────────────────────
    # 读取配置中的股票池分层
    anchor_info = config.get('anchor', {})
    anchor_name_pool = anchor_info.get('name', '铂力特')
    
    core_universe = config.get('core_universe', [])
    research_core = config.get('research_core', [])
    extended_universe = config.get('extended_universe', [])
    trading_candidates = config.get('trading_candidates', [])
    research_candidates = config.get('research_candidates', [])
    
    # 整理各层股票列表
    def format_stock_list(stocks, max_show=5):
        if not stocks:
            return "无"
        names = [s.get('name', s.get('code', '')) for s in stocks]
        if len(names) <= max_show:
            return " / ".join(names)
        return f"{' / '.join(names[:max_show])}...（共{len(names)}只）"
    
    section_pool = f"""## 八、股票池快照

- **anchor**：{anchor_name_pool}
- **trading_core**：{format_stock_list(core_universe)}
- **research_core**：{format_stock_list(research_core)}
- **trading_candidates**：{format_stock_list(trading_candidates)}
- **research_candidates**：{format_stock_list(research_candidates)}
- **extended_watchlist**：{format_stock_list(extended_universe)}

> **计算口径说明**：当前板块均值、排名、rolling 只基于 **trading_core**（benchmark_included=true）计算；其他层仅用于研究或观察，不参与当前 benchmark。
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
    print(f"  板块排名  : 涨跌幅 {fmt_rank(return_rank_in_sector, rank_total) if isinstance(rank_total, int) else 'N/A'}  |  成交额 {fmt_rank(amount_rank_in_sector, rank_total) if isinstance(rank_total, int) else 'N/A'}")
    if abnormal_signals:
        print(f"  异常信号  : {' | '.join(abnormal_signals)}")
    else:
        print(f"  异常信号  : 无")
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
