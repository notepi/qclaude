"""
连续观察层模块 v2.4 P1
从 archive/metrics/ 读取最近 N 日数据，计算价格和量能的连续性指标

职责：
  - 读取 archive/metrics/YYYYMMDD.parquet（最近 window_days 日）
  - 计算价格连续性指标（rs_* 系列）
  - 计算量能连续性指标（volume_* 系列）
  - 输出连续性标签（price_trend_label / volume_trend_label / momentum_label）
  - 生成规则化短周期摘要（trend_summary）
  - 保存 data/price/analytics/rolling_metrics.parquet

不做：
  - 事件连续性（P2）
  - 复杂统计检验
  - 趋势预测
"""

import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from src.shared.storage import Storage
from src.shared.paths import ARCHIVE_METRICS_DIR, CONFIG_DIR
from src.shared.config import load_config

# 存储层
STORAGE = Storage("price")

DEFAULT_OUTPUT_PATH = STORAGE.analytics_dir / "rolling_metrics.parquet"

# 默认观察窗口（交易日）
DEFAULT_WINDOW = 5


def _get_universe_version() -> str:
    """从 stocks.yaml 读取股票池版本，缺失时返回 'unknown'"""
    try:
        cfg = load_config()
        return str(cfg.get("version", "unknown"))
    except Exception:
        return "unknown"


# ─────────────────────────────────────────────
# 数据读取
# ─────────────────────────────────────────────

def load_archive_metrics(window_days: int = DEFAULT_WINDOW) -> pd.DataFrame:
    """
    从 archive/metrics/ 读取最近 window_days 日的指标数据

    Returns:
        DataFrame，按 trade_date 升序排列（旧→新），最多 window_days 行
        如果 archive 为空或不存在，返回空 DataFrame
    """
    if not ARCHIVE_METRICS_DIR.exists():
        return pd.DataFrame()

    files = sorted(ARCHIVE_METRICS_DIR.glob("*.parquet"))
    if not files:
        return pd.DataFrame()

    # 取最近 window_days 个文件
    recent_files = files[-window_days:]

    dfs = []
    for f in recent_files:
        try:
            df = pd.read_parquet(f)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"[WARN] rolling_analyzer: 读取 {f.name} 失败: {e}")

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)

    # 确保 trade_date 是 datetime
    if combined["trade_date"].dtype.kind != "M":
        combined["trade_date"] = pd.to_datetime(combined["trade_date"])

    # 去重（同一天可能有多条），保留最新
    combined = combined.sort_values("trade_date").drop_duplicates(
        subset=["trade_date"], keep="last"
    )

    return combined.reset_index(drop=True)


# ─────────────────────────────────────────────
# 价格连续性指标
# ─────────────────────────────────────────────

def compute_price_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    计算价格连续性指标

    输入：按日期升序排列的 DataFrame（旧→新）
    输出：指标字典
    """
    if df.empty or "relative_strength" not in df.columns:
        return {
            "rs_outperform_days_5d": None,
            "rs_consecutive_outperform": None,
            "rs_consecutive_underperform": None,
            "rs_5d_mean": None,
            "rs_5d_series": [],
        }

    rs_series = df["relative_strength"].tolist()  # 旧→新

    # rs_5d_series：最近5日序列（旧→新）
    rs_5d_series = [round(v, 4) if v is not None else None for v in rs_series]

    # rs_outperform_days_5d：跑赢板块（rs > 0）的天数
    rs_outperform_days = sum(1 for v in rs_series if v is not None and v > 0)

    # rs_5d_mean：均值
    valid_rs = [v for v in rs_series if v is not None]
    rs_5d_mean = round(sum(valid_rs) / len(valid_rs), 4) if valid_rs else None

    # rs_consecutive_outperform：从最新日往前数，连续跑赢天数
    rs_consecutive_outperform = 0
    for v in reversed(rs_series):
        if v is not None and v > 0:
            rs_consecutive_outperform += 1
        else:
            break

    # rs_consecutive_underperform：从最新日往前数，连续跑输天数
    rs_consecutive_underperform = 0
    for v in reversed(rs_series):
        if v is not None and v <= 0:
            rs_consecutive_underperform += 1
        else:
            break

    return {
        "rs_outperform_days_5d": rs_outperform_days,
        "rs_consecutive_outperform": rs_consecutive_outperform,
        "rs_consecutive_underperform": rs_consecutive_underperform,
        "rs_5d_mean": rs_5d_mean,
        "rs_5d_series": rs_5d_series,
    }


# ─────────────────────────────────────────────
# 量能连续性指标
# ─────────────────────────────────────────────

def compute_volume_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    计算量能连续性指标

    输入：按日期升序排列的 DataFrame（旧→新）
    输出：指标字典
    """
    empty = {
        "volume_expand_days_5d": None,
        "amount_20d_high_days_5d": None,
        "volume_consecutive_shrink": None,
        "volume_consecutive_expand": None,
        "amount_vs_5d_avg_series": [],
    }

    if df.empty:
        return empty

    # amount_vs_5d_avg 序列
    if "amount_vs_5d_avg" not in df.columns:
        return empty

    avg_series = df["amount_vs_5d_avg"].tolist()  # 旧→新

    # volume_expand_days_5d：放量天数（amount_vs_5d_avg > 1.0）
    volume_expand_days = sum(
        1 for v in avg_series if v is not None and v > 1.0
    )

    # amount_20d_high_days_5d：成交额创20日新高的天数
    amount_20d_high_days = 0
    if "amount_20d_high" in df.columns:
        amount_20d_high_days = int(df["amount_20d_high"].sum())

    # volume_consecutive_shrink：从最新日往前数，连续缩量天数（< 0.8）
    volume_consecutive_shrink = 0
    for v in reversed(avg_series):
        if v is not None and v < 0.8:
            volume_consecutive_shrink += 1
        else:
            break

    # volume_consecutive_expand：从最新日往前数，连续放量天数（> 1.2）
    volume_consecutive_expand = 0
    for v in reversed(avg_series):
        if v is not None and v > 1.2:
            volume_consecutive_expand += 1
        else:
            break

    # amount_vs_5d_avg_series：序列（旧→新）
    avg_series_rounded = [
        round(v, 2) if v is not None else None for v in avg_series
    ]

    return {
        "volume_expand_days_5d": volume_expand_days,
        "amount_20d_high_days_5d": amount_20d_high_days,
        "volume_consecutive_shrink": volume_consecutive_shrink,
        "volume_consecutive_expand": volume_consecutive_expand,
        "amount_vs_5d_avg_series": avg_series_rounded,
    }


# ─────────────────────────────────────────────
# 主力资金连续性指标（v2.5C P1）
# ─────────────────────────────────────────────

def compute_capital_flow_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    计算主力资金连续性指标。

    依赖字段：net_mf_amount（万元，来自 archive/metrics）
    阈值：> +200万 视为净流入，< -200万 视为净流出

    Returns:
        mf_inflow_days_5d       近N日主力净流入天数
        mf_consecutive_inflow   从今日往前连续净流入天数
        mf_consecutive_outflow  从今日往前连续净流出天数
        mf_5d_mean              近N日主力净流入均值（万元）
        capital_flow_trend_label 连续资金趋势标签
    """
    empty = {
        "mf_inflow_days_5d":        None,
        "mf_consecutive_inflow":    None,
        "mf_consecutive_outflow":   None,
        "mf_5d_mean":               None,
        "capital_flow_trend_label": "资金数据不足",
    }

    if df.empty or "net_mf_amount" not in df.columns:
        return empty

    mf_series = df["net_mf_amount"].tolist()  # 旧→新

    # 过滤掉 None/NaN
    valid_series = [v for v in mf_series if v is not None and not (isinstance(v, float) and v != v)]
    if not valid_series:
        return empty

    INFLOW_THRESHOLD  =  200.0   # 万元，视为净流入
    OUTFLOW_THRESHOLD = -200.0   # 万元，视为净流出

    # mf_inflow_days_5d：净流入天数
    mf_inflow_days = sum(1 for v in valid_series if v > INFLOW_THRESHOLD)

    # mf_5d_mean：均值
    mf_5d_mean = round(sum(valid_series) / len(valid_series), 1)

    # mf_consecutive_inflow：从最新日往前连续净流入天数
    mf_consecutive_inflow = 0
    for v in reversed(valid_series):
        if v > INFLOW_THRESHOLD:
            mf_consecutive_inflow += 1
        else:
            break

    # mf_consecutive_outflow：从最新日往前连续净流出天数
    mf_consecutive_outflow = 0
    for v in reversed(valid_series):
        if v < OUTFLOW_THRESHOLD:
            mf_consecutive_outflow += 1
        else:
            break

    # capital_flow_trend_label
    n = len(valid_series)
    outflow_days = sum(1 for v in valid_series if v < OUTFLOW_THRESHOLD)

    # 转折信号优先（今日刚发生转折）
    prev_series = valid_series[:-1]  # 去掉今日
    today_val   = valid_series[-1]

    prev_outflow_streak = 0
    for v in reversed(prev_series):
        if v < OUTFLOW_THRESHOLD:
            prev_outflow_streak += 1
        else:
            break

    prev_inflow_streak = 0
    for v in reversed(prev_series):
        if v > INFLOW_THRESHOLD:
            prev_inflow_streak += 1
        else:
            break

    if prev_outflow_streak >= 2 and today_val > INFLOW_THRESHOLD:
        label = "资金开始回流"
    elif prev_inflow_streak >= 2 and today_val < OUTFLOW_THRESHOLD:
        label = "资金开始转弱"
    elif mf_consecutive_outflow >= 3 or (outflow_days >= 4 and mf_5d_mean < -500):
        label = "资金持续流出"
    elif mf_inflow_days >= 4 and mf_5d_mean > 500:
        label = "资金持续流入"
    else:
        label = "资金状态中性"

    return {
        "mf_inflow_days_5d":        mf_inflow_days,
        "mf_consecutive_inflow":    mf_consecutive_inflow,
        "mf_consecutive_outflow":   mf_consecutive_outflow,
        "mf_5d_mean":               mf_5d_mean,
        "capital_flow_trend_label": label,
    }


# ─────────────────────────────────────────────
# 连续性标签
# ─────────────────────────────────────────────

def compute_price_trend_label(pm: Dict[str, Any]) -> str:
    """
    价格连续性标签

    优先级：连续强于板块 > 连续弱于板块 > 短期相对强势 > 短期走弱 > 短期中性
    """
    if pm.get("rs_consecutive_outperform") is None:
        return "数据不足"

    consec_out = pm["rs_consecutive_outperform"]
    consec_under = pm["rs_consecutive_underperform"]
    outperform_days = pm["rs_outperform_days_5d"]

    if consec_out >= 3:
        return "连续强于板块"
    if consec_under >= 3:
        return "连续弱于板块"
    if outperform_days >= 3:
        return "短期相对强势"
    if consec_under >= 2:
        return "短期走弱"
    return "短期中性"


def compute_volume_trend_label(vm: Dict[str, Any]) -> str:
    """
    量能连续性标签

    优先级：持续放量 > 持续缩量 > 放量增强 > 缩量企稳 > 量能平稳
    """
    if vm.get("volume_consecutive_expand") is None:
        return "数据不足"

    consec_expand = vm["volume_consecutive_expand"]
    consec_shrink = vm["volume_consecutive_shrink"]
    expand_days = vm["volume_expand_days_5d"]
    avg_series = vm.get("amount_vs_5d_avg_series", [])

    if consec_expand >= 3:
        return "持续放量"
    if consec_shrink >= 3:
        return "持续缩量"
    if expand_days >= 3:
        return "放量增强"

    # 缩量企稳：连续缩量1~2日，但最新日较前日回升
    if 1 <= consec_shrink <= 2:
        valid = [v for v in avg_series if v is not None]
        if len(valid) >= 2 and valid[-1] > valid[-2]:
            return "缩量企稳"

    return "量能平稳"


def compute_momentum_label(price_label: str, volume_label: str) -> str:
    """
    综合动量标签（价格 × 量能矩阵）
    """
    strong_price = price_label in ("连续强于板块", "短期相对强势")
    weak_price = price_label in ("连续弱于板块", "短期走弱")
    strong_volume = volume_label in ("持续放量", "放量增强")
    weak_volume = volume_label in ("持续缩量",)

    if strong_price and strong_volume:
        return "量价齐升"
    if strong_price and volume_label in ("量能平稳", "缩量企稳"):
        return "价强量稳"
    if strong_price and weak_volume:
        return "价强量弱"
    if weak_price and weak_volume:
        return "量价齐弱"
    if weak_price and strong_volume:
        return "放量下跌"
    return "短期震荡"


# ─────────────────────────────────────────────
# 短周期摘要
# ─────────────────────────────────────────────

def _fmt_pct(v: Optional[float]) -> str:
    """格式化为百分比字符串，带符号"""
    if v is None:
        return "N/A"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.2f}%"


def _fmt_series(series: List, suffix: str = "") -> str:
    """将序列格式化为单行展示"""
    parts = []
    for i, v in enumerate(series):
        if v is None:
            parts.append("N/A")
        else:
            if suffix == "%":
                sign = "+" if v >= 0 else ""
                parts.append(f"{sign}{v * 100:.1f}%")
            elif suffix == "x":
                parts.append(f"{v:.2f}x")
            else:
                parts.append(str(v))
    return " → ".join(parts)


def build_trend_summary(
    pm: Dict[str, Any],
    vm: Dict[str, Any],
    momentum_label: str,
    available_days: int,
) -> str:
    """
    生成规则化短周期摘要（2~3句）

    规则：
      状态句（必选）+ 量能句（有明显特征时）
    """
    if available_days < 2:
        return f"历史数据不足（当前仅 {available_days} 日归档），近5日状态暂不可用。"

    sentences = []

    # ── 状态句（必选）──
    rs_mean_str = _fmt_pct(pm.get("rs_5d_mean"))
    n = available_days

    state_map = {
        "量价齐升": f"近{n}日铂力特持续跑赢板块，均相对强弱 {rs_mean_str}，量价配合良好。",
        "价强量稳": f"近{n}日铂力特相对板块偏强，均相对强弱 {rs_mean_str}，成交量维持平稳。",
        "价强量弱": f"近{n}日铂力特相对板块偏强，均相对强弱 {rs_mean_str}，但成交量持续萎缩，强势持续性存疑。",
        "量价齐弱": f"近{n}日铂力特持续跑输板块，均相对强弱 {rs_mean_str}，量能同步萎缩。",
        "放量下跌": f"近{n}日铂力特跑输板块，均相对强弱 {rs_mean_str}，但成交量有所放大，需关注是否有资金出逃。",
        "短期震荡": f"近{n}日铂力特相对板块表现中性，均相对强弱 {rs_mean_str}，方向不明。",
    }
    sentences.append(state_map.get(momentum_label, f"近{n}日综合动量：{momentum_label}，均相对强弱 {rs_mean_str}。"))

    # ── 量能句（有明显特征时）──
    high_days = vm.get("amount_20d_high_days_5d", 0) or 0
    consec_shrink = vm.get("volume_consecutive_shrink", 0) or 0
    consec_expand = vm.get("volume_consecutive_expand", 0) or 0

    if high_days >= 2:
        sentences.append(f"近{n}日内 {high_days} 次成交额创20日新高，量能活跃。")
    elif consec_shrink >= 3:
        sentences.append(f"成交额已连续 {consec_shrink} 日萎缩，市场关注度下降。")
    elif consec_expand >= 3:
        sentences.append(f"成交额已连续 {consec_expand} 日放大，资金持续流入。")

    return "".join(sentences)


def build_rolling_summary_text(
    pm: Dict[str, Any],
    vm: Dict[str, Any],
    mf: Dict[str, Any],
    momentum_label: str,
    available_days: int,
) -> str:
    """
    v2.7 新增：生成压缩版近N日结构摘要（供 reporter 直接使用）

    格式：
    跑赢板块 4/5 日，平均相对强弱 -0.59%，属于"胜率尚可，但幅度偏弱"；量能 连续缩量；资金面有改善迹象。
    """
    if available_days < 2:
        return f"历史数据不足（当前仅 {available_days} 日归档），近5日状态暂不可用。"

    n = available_days

    # 跑赢天数
    out_days = pm.get("rs_outperform_days_5d", 0) or 0
    rs_mean = pm.get("rs_5d_mean")
    rs_mean_str = _fmt_pct(rs_mean)

    # 结构描述
    if out_days >= 4:
        structure_desc = "胜率较高"
    elif out_days >= 3:
        if rs_mean is not None and rs_mean > 0:
            structure_desc = "胜率尚可，幅度占优"
        else:
            structure_desc = "胜率尚可，但幅度偏弱"
    elif out_days >= 2:
        structure_desc = "结构中性"
    else:
        structure_desc = "胜率偏低"

    # 量能描述
    consec_shrink = vm.get("volume_consecutive_shrink", 0) or 0
    consec_expand = vm.get("volume_consecutive_expand", 0) or 0
    volume_trend_label = vm.get("volume_trend_label", "")

    if consec_shrink >= 2:
        volume_desc = "连续缩量"
    elif consec_expand >= 2:
        volume_desc = "连续放量"
    elif volume_trend_label == "量能平稳":
        volume_desc = "量能平稳"
    else:
        volume_desc = volume_trend_label or "量能平稳"

    # 资金描述
    mf_inflow_days = mf.get("mf_inflow_days_5d")
    cf_trend_label = mf.get("capital_flow_trend_label", "")

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


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def compute_rolling_metrics(
    window_days: int = DEFAULT_WINDOW,
    output_path: str = None,
) -> Optional[Dict[str, Any]]:
    """
    计算连续观察层指标，保存 rolling_metrics.parquet

    Args:
        window_days: 观察窗口（交易日数），默认5
        output_path: 输出路径，默认 data/analytics/rolling_metrics.parquet

    Returns:
        指标字典（供 reporter 直接使用），失败时返回 None
    """
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH

    # 读取 archive
    df = load_archive_metrics(window_days)
    available_days = len(df)

    if available_days == 0:
        print("[WARN] rolling_analyzer: archive/metrics/ 无数据，跳过连续观察层")
        return None

    if available_days < 2:
        print(f"[WARN] rolling_analyzer: 仅有 {available_days} 日归档数据，连续性指标意义有限")

    # 取最新交易日
    latest_date = pd.Timestamp(df["trade_date"].iloc[-1]).strftime("%Y%m%d")

    universe_version = _get_universe_version()

    # 计算指标
    pm = compute_price_metrics(df)
    vm = compute_volume_metrics(df)
    mf = compute_capital_flow_metrics(df)

    price_label = compute_price_trend_label(pm)
    volume_label = compute_volume_trend_label(vm)
    momentum_label = compute_momentum_label(price_label, volume_label)
    trend_summary = build_trend_summary(pm, vm, momentum_label, available_days)
    rolling_summary_text = build_rolling_summary_text(pm, vm, mf, momentum_label, available_days)

    result = {
        "trade_date": latest_date,
        "window_days": window_days,
        "available_days": available_days,
        # 价格连续性
        "rs_outperform_days_5d": pm["rs_outperform_days_5d"],
        "rs_consecutive_outperform": pm["rs_consecutive_outperform"],
        "rs_consecutive_underperform": pm["rs_consecutive_underperform"],
        "rs_5d_mean": pm["rs_5d_mean"],
        "rs_5d_series": json.dumps(pm["rs_5d_series"]),  # parquet 不支持 list，序列化存储
        # 量能连续性
        "volume_expand_days_5d": vm["volume_expand_days_5d"],
        "amount_20d_high_days_5d": vm["amount_20d_high_days_5d"],
        "volume_consecutive_shrink": vm["volume_consecutive_shrink"],
        "volume_consecutive_expand": vm["volume_consecutive_expand"],
        "amount_vs_5d_avg_series": json.dumps(vm["amount_vs_5d_avg_series"]),
        # 标签
        "price_trend_label": price_label,
        "volume_trend_label": volume_label,
        "momentum_label": momentum_label,
        # 摘要
        "trend_summary": trend_summary,
        "rolling_summary_text": rolling_summary_text,
        # v2.5C P1 新增：主力资金连续性
        "mf_inflow_days_5d":       mf["mf_inflow_days_5d"],
        "mf_consecutive_inflow":   mf["mf_consecutive_inflow"],
        "mf_consecutive_outflow":  mf["mf_consecutive_outflow"],
        "mf_5d_mean":              mf["mf_5d_mean"],
        "capital_flow_trend_label": mf["capital_flow_trend_label"],
        # v2.5A 新增
        "universe_version": universe_version,
        # 元信息
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # 保存
    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([result]).to_parquet(out_file, index=False)

    print(f"[INFO] rolling_analyzer: {available_days} 日数据 → 连续观察层计算完成")
    print(f"[INFO] rolling_analyzer: 价格={price_label} | 量能={volume_label} | 动量={momentum_label}")
    print(f"[INFO] rolling_analyzer: 输出至 {out_file}")

    # reporter 用的原始序列（不序列化）
    result["rs_5d_series"] = pm["rs_5d_series"]
    result["amount_vs_5d_avg_series"] = vm["amount_vs_5d_avg_series"]

    return result


def load_rolling_metrics(path: str = None) -> Optional[Dict[str, Any]]:
    """
    加载已有的 rolling_metrics（供 reporter 使用）

    Returns:
        指标字典，文件不存在时返回 None
    """
    if path is None:
        path = DEFAULT_OUTPUT_PATH

    f = Path(path)
    if not f.exists():
        return None

    try:
        df = pd.read_parquet(f)
        if df.empty:
            return None
        row = df.iloc[0].to_dict()
        # 反序列化序列字段
        for key in ("rs_5d_series", "amount_vs_5d_avg_series"):
            if key in row and isinstance(row[key], str):
                row[key] = json.loads(row[key])
        return row
    except Exception as e:
        print(f"[WARN] rolling_analyzer: 加载 rolling_metrics 失败: {e}")
        return None


def main():
    """独立运行（调试用）"""
    print("=" * 50)
    print("连续观察层（rolling_analyzer v2.4）")
    print("=" * 50)
    result = compute_rolling_metrics()
    if result:
        print(f"\n摘要：{result['trend_summary']}")
        print(f"rs序列：{result['rs_5d_series']}")
        print(f"量能序列：{result['amount_vs_5d_avg_series']}")


if __name__ == "__main__":
    main()
