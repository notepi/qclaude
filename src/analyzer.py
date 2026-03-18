"""
指标计算模块 v2.1.1
计算铂力特核心复盘指标，支持板块内排名、标签化结论、异常信号

配置说明：
- anchor_symbol: 锚定标的（铂力特），不参与板块均值计算
- core_universe: 核心股票池，用于板块均值计算和排名
- extended_universe: 扩展股票池，暂不参与板块均值计算

v2.1 新增：
- return_rank_in_sector: 铂力特在 core_universe 中的涨跌幅排名（第1名=最高）
- amount_rank_in_sector: 铂力特在 core_universe 中的成交额排名（第1名=最高）
- sector_total_size: core_universe 样本总数（含铂力特）
- price_strength_label: 强 / 中 / 弱
- volume_strength_label: 强 / 中 / 弱
- overall_signal_label: 强 / 中性偏强 / 中性 / 中性偏弱 / 弱
- abnormal_signals: 异常信号列表
"""

import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional


# 项目根目录（基于本文件位置计算）
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_NORMALIZED_DIR = PROJECT_ROOT / "data" / "normalized"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

# 默认路径
DEFAULT_CONFIG_PATH = CONFIG_DIR / "stocks.yaml"
DEFAULT_DATA_PATH = DATA_NORMALIZED_DIR / "market_data_normalized.parquet"  # v2.3: 从 normalized 层读取
DEFAULT_RAW_DATA_PATH = DATA_RAW_DIR / "market_data.parquet"                # 备用：raw 层
DEFAULT_OUTPUT_PATH = DATA_PROCESSED_DIR / "daily_metrics.parquet"


# ─────────────────────────────────────────────
# 标签规则（固化，不依赖自然语言判断）
# ─────────────────────────────────────────────

def compute_price_strength_label(
    anchor_return: Optional[float],
    relative_strength: Optional[float],
    return_rank_in_sector: Optional[int],
    sector_total_size: int
) -> str:
    """
    价格强度标签规则（固化）：

    强：满足以下任意一条
      - anchor_return > 2%
      - relative_strength > 1% 且 return_rank_in_sector <= sector_total_size // 2

    弱：满足以下任意一条
      - anchor_return < -2%
      - relative_strength < -1% 且 return_rank_in_sector > sector_total_size // 2

    中：其余情况
    """
    if anchor_return is None or relative_strength is None or return_rank_in_sector is None:
        return "中"

    half = max(sector_total_size // 2, 1)

    if anchor_return > 0.02 or (relative_strength > 0.01 and return_rank_in_sector <= half):
        return "强"
    if anchor_return < -0.02 or (relative_strength < -0.01 and return_rank_in_sector > half):
        return "弱"
    return "中"


def compute_volume_strength_label(
    amount_vs_5d_avg: Optional[float],
    amount_20d_high: Optional[bool],
    amount_rank_in_sector: Optional[int],
    sector_total_size: int
) -> str:
    """
    成交额强度标签规则（固化）：

    强：满足以下任意一条
      - amount_20d_high = True
      - amount_vs_5d_avg > 1.5
      - amount_rank_in_sector <= sector_total_size // 2

    弱：满足以下全部条件
      - amount_vs_5d_avg < 0.7
      - amount_20d_high = False
      - amount_rank_in_sector > sector_total_size // 2

    中：其余情况
    """
    if amount_vs_5d_avg is None or amount_20d_high is None or amount_rank_in_sector is None:
        return "中"

    half = max(sector_total_size // 2, 1)

    if amount_20d_high or amount_vs_5d_avg > 1.5 or amount_rank_in_sector <= half:
        return "强"
    if amount_vs_5d_avg < 0.7 and not amount_20d_high and amount_rank_in_sector > half:
        return "弱"
    return "中"


def compute_overall_signal_label(
    price_strength_label: str,
    volume_strength_label: str
) -> str:
    """
    综合信号标签规则（固化，基于价格强度 × 成交额强度矩阵）：

    价格\成交额  强          中          弱
    强          强          中性偏强    中性偏强
    中          中性偏强    中性        中性偏弱
    弱          中性偏弱    中性偏弱    弱

    规则说明：
    - 强 = 价格强 + 成交额强
    - 中性偏强 = 价格强+成交额中/弱，或 价格中+成交额强
    - 中性 = 价格中+成交额中
    - 中性偏弱 = 价格中+成交额弱，或 价格弱+成交额强/中
    - 弱 = 价格弱+成交额弱
    """
    matrix = {
        ("强", "强"):  "强",
        ("强", "中"):  "中性偏强",
        ("强", "弱"):  "中性偏强",
        ("中", "强"):  "中性偏强",
        ("中", "中"):  "中性",
        ("中", "弱"):  "中性偏弱",
        ("弱", "强"):  "中性偏弱",
        ("弱", "中"):  "中性偏弱",
        ("弱", "弱"):  "弱",
    }
    return matrix.get((price_strength_label, volume_strength_label), "中性")


def compute_abnormal_signals(
    relative_strength: Optional[float],
    amount_vs_5d_avg: Optional[float],
    amount_20d_high: Optional[bool],
    return_rank_in_sector: Optional[int],
    amount_rank_in_sector: Optional[int]
) -> List[str]:
    """
    异常信号检测规则（固化，v2.1.1）：

    触发条件 → 信号文本：
    1. relative_strength > 1%         → "跑赢板块明显"
    2. relative_strength < -1%        → "跑输板块明显"
    3. amount_vs_5d_avg > 1.5         → "成交额显著放大"
    4. amount_20d_high = True         → "成交额创20日新高"
    5. return_rank_in_sector == 1     → "涨幅居板块首位"
       return_rank_in_sector == 2     → "涨幅位居板块前二"
    6. amount_rank_in_sector == 1     → "成交额居板块首位"
       amount_rank_in_sector == 2     → "成交额位居板块前二"

    排名信号说明：
    - 不再使用固定阈值（如 <= 3），改为绝对名次触发
    - rank=1 和 rank=2 分别有独立文案，语义更精确
    - 避免样本数少时（如 4 只）rank=3/4 也触发"前列"的误判

    返回：触发的信号列表（可为空）
    """
    signals = []

    if relative_strength is not None:
        if relative_strength > 0.01:
            signals.append("跑赢板块明显")
        elif relative_strength < -0.01:
            signals.append("跑输板块明显")

    if amount_vs_5d_avg is not None and amount_vs_5d_avg > 1.5:
        signals.append("成交额显著放大")

    if amount_20d_high:
        signals.append("成交额创20日新高")

    if return_rank_in_sector is not None:
        if return_rank_in_sector == 1:
            signals.append("涨幅居板块首位")
        elif return_rank_in_sector == 2:
            signals.append("涨幅位居板块前二")

    if amount_rank_in_sector is not None:
        if amount_rank_in_sector == 1:
            signals.append("成交额居板块首位")
        elif amount_rank_in_sector == 2:
            signals.append("成交额位居板块前二")

    return signals


# ─────────────────────────────────────────────
# 数据加载
# ─────────────────────────────────────────────

def load_config(config_path: str = None) -> Dict[str, Any]:
    """加载配置文件"""
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_file}")

    with open(config_file, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_universe_version(config: Dict[str, Any]) -> str:
    """
    从 config 读取股票池版本号。
    stocks.yaml 中 version 字段缺失时返回 "unknown"。
    """
    return str(config.get("version", "unknown"))

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"配置文件格式错误: {e}")

    if 'anchor' not in config:
        raise ValueError("配置文件缺少必要字段: anchor")
    if 'code' not in config['anchor']:
        raise ValueError("配置文件缺少必要字段: anchor.code")
    if 'core_universe' not in config or not config['core_universe']:
        raise ValueError("配置文件缺少必要字段: core_universe 或字段为空")

    return config


def load_market_data(data_path: str = None) -> pd.DataFrame:
    """
    加载市场数据

    v2.3：优先从 normalized 层读取；normalized 不存在时自动生成。
    normalized 层保证：
      - ts_code 含交易所后缀
      - trade_date 为 datetime64[ns]
      - amount 单位为千元（Tushare 原始，未转换）
      - 无效行（close=0/null）已过滤

    Args:
        data_path: 数据文件路径，None 时使用 normalized 层默认路径

    Raises:
        FileNotFoundError: raw 和 normalized 均不存在
        ValueError: 数据格式异常
    """
    if data_path is None:
        # 优先 normalized，不存在则自动从 raw 生成
        try:
            from normalizer import load_normalized
            df = load_normalized()
        except Exception as e:
            print(f"[WARN] normalized 层加载失败（{e}），降级到 raw 层")
            data_path = DEFAULT_RAW_DATA_PATH

    if data_path is not None:
        data_file = Path(data_path)
        if not data_file.exists():
            raise FileNotFoundError(f"市场数据文件不存在: {data_file}")
        try:
            df = pd.read_parquet(data_file)
        except Exception as e:
            raise ValueError(f"读取市场数据失败: {e}")

    if df.empty:
        raise ValueError("市场数据为空")

    required_cols = ["ts_code", "trade_date", "close", "amount"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"市场数据缺少必要列: {missing_cols}")

    # 确保 trade_date 是 datetime（normalized 层已保证，raw 层需转换）
    if df["trade_date"].dtype.kind != "M":
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")

    return df


# ─────────────────────────────────────────────
# 核心分析
# ─────────────────────────────────────────────

def analyze_anchor_symbol(
    config_path: str = None,
    data_path: str = None,
    as_of_date=None,
) -> Dict[str, Any]:
    """
    分析锚定标的（铂力特）的核心指标

    v2.1 新增字段：
        - return_rank_in_sector: 铂力特在 core_universe（含铂力特）中的涨跌幅排名
        - amount_rank_in_sector: 铂力特在 core_universe（含铂力特）中的成交额排名
        - sector_total_size: 参与排名的样本总数（core_universe + 铂力特）
        - price_strength_label: 强 / 中 / 弱
        - volume_strength_label: 强 / 中 / 弱
        - overall_signal_label: 强 / 中性偏强 / 中性 / 中性偏弱 / 弱
        - abnormal_signals: 异常信号列表

    注意：
        - 板块均值（sector_avg_return）仍只基于 core_universe 计算，不含铂力特
        - 排名（return_rank_in_sector / amount_rank_in_sector）基于 core_universe + 铂力特
          共同排名，sector_total_size = core_universe 数量 + 1

    Args:
        as_of_date: 指定计算日期（str "YYYYMMDD" / "YYYY-MM-DD" 或 datetime/Timestamp）。
                    非 None 时，数据截断到该日期（含），以该日为"最新日"计算指标。
                    用于 backfill 场景。
    """
    # 1. 加载配置
    config = load_config(config_path)
    anchor_symbol = config['anchor']['code']

    core_universe = config.get('core_universe', [])
    core_codes = [stock['code'] for stock in core_universe]
    # 排除锚定标的（双重保险）
    core_codes = [code for code in core_codes if code != anchor_symbol]
    core_total_count = len(core_codes)

    # 2. 加载市场数据
    df = load_market_data(data_path)

    # as_of_date 截断：只保留 <= 指定日期的数据
    if as_of_date is not None:
        cutoff = pd.Timestamp(as_of_date)
        df = df[df['trade_date'] <= cutoff].copy()
        if df.empty:
            raise ValueError(f"as_of_date={as_of_date} 之前无任何市场数据，无法计算指标。")

    # 3. 锚定标的数据
    anchor_df = df[df['ts_code'] == anchor_symbol].copy()
    if anchor_df.empty:
        raise ValueError(
            f"锚定标的 [{anchor_symbol}] 数据为空，无法计算指标。\n"
            f"请检查配置文件中的 anchor_symbol 是否正确，以及数据文件中是否包含该股票数据。"
        )

    anchor_df = anchor_df.sort_values('trade_date').reset_index(drop=True)
    latest = anchor_df.iloc[-1]
    latest_date = latest['trade_date']

    # A. 锚定标的当日涨跌幅
    if len(anchor_df) >= 2:
        prev_close = anchor_df.iloc[-2]['close']
        curr_close = latest['close']
        anchor_return = (curr_close - prev_close) / prev_close
    else:
        raise ValueError(
            f"锚定标的 [{anchor_symbol}] 数据不足（仅 {len(anchor_df)} 条），"
            f"无法计算涨跌幅。至少需要 2 个交易日的数据。"
        )

    # B. 板块各股数据（core_universe，用于均值 + 排名）
    sector_returns = []       # 仅 core_universe，用于均值
    sector_amounts = []       # 仅 core_universe，用于排名
    missing_on_latest_date = []

    # 收集 core_universe 各股当日涨跌幅和成交额
    core_stock_returns: Dict[str, float] = {}
    core_stock_amounts: Dict[str, float] = {}

    for code in core_codes:
        stock_df = df[df['ts_code'] == code].copy()
        stock_df = stock_df.sort_values('trade_date').reset_index(drop=True)

        latest_stock = stock_df[stock_df['trade_date'] == latest_date]
        if len(latest_stock) == 0:
            missing_on_latest_date.append(code)
            continue
        latest_stock_row = latest_stock.iloc[0]

        latest_idx = stock_df[stock_df['trade_date'] == latest_date].index[0]
        if latest_idx > 0:
            prev_close = stock_df.iloc[latest_idx - 1]['close']
            curr_close = latest_stock_row['close']
            daily_return = (curr_close - prev_close) / prev_close
            core_stock_returns[code] = daily_return
            sector_returns.append(daily_return)

        # 成交额（不需要前一日，直接取当日）
        core_stock_amounts[code] = latest_stock_row['amount']
        sector_amounts.append(latest_stock_row['amount'])

    # 覆盖度检查
    participated_count = len(sector_returns)
    coverage_ratio = participated_count / core_total_count if core_total_count > 0 else 0

    print(f"[INFO] 板块计算（core_universe）: {participated_count}/{core_total_count} 只股票参与 ({coverage_ratio:.1%})")
    if missing_on_latest_date:
        print(f"[WARN] 以下股票在 {latest_date.strftime('%Y-%m-%d')} 无数据: {missing_on_latest_date}")

    if coverage_ratio < 0.5:
        raise ValueError(
            f"板块覆盖度不足: 仅 {participated_count}/{core_total_count} ({coverage_ratio:.1%}) "
            f"股票在 {latest_date.strftime('%Y-%m-%d')} 有有效数据（要求 >= 50%）。\n"
            f"缺失股票: {missing_on_latest_date}"
        )

    # C. 板块均值（仅 core_universe，不含铂力特）
    sector_avg_return = sum(sector_returns) / len(sector_returns) if sector_returns else None

    # D. 相对强弱
    relative_strength = (
        anchor_return - sector_avg_return
        if anchor_return is not None and sector_avg_return is not None
        else None
    )

    # E. 成交额指标
    anchor_amount = latest['amount']

    if len(anchor_df) >= 20:
        last_20_amounts = anchor_df.iloc[-20:]['amount']
        amount_20d_high = bool(anchor_amount == last_20_amounts.max())
    elif len(anchor_df) >= 1:
        amount_20d_high = bool(anchor_amount == anchor_df['amount'].max())
    else:
        amount_20d_high = None

    if len(anchor_df) >= 6:
        prev_5d_avg = anchor_df.iloc[-6:-1]['amount'].mean()
        amount_vs_5d_avg = anchor_amount / prev_5d_avg if prev_5d_avg > 0 else None
    else:
        amount_vs_5d_avg = None

    # ─────────────────────────────────────────
    # v2.1 新增：板块内排名
    # 排名基于 core_universe + 铂力特，共同排名
    # sector_total_size = core_universe 有效数量 + 1（铂力特）
    # ─────────────────────────────────────────

    # 涨跌幅排名（第1名=最高）
    all_returns_for_rank = dict(core_stock_returns)  # core_universe 各股
    all_returns_for_rank[anchor_symbol] = anchor_return  # 加入铂力特

    sorted_returns = sorted(all_returns_for_rank.items(), key=lambda x: x[1], reverse=True)
    return_rank_in_sector = next(
        (i + 1 for i, (code, _) in enumerate(sorted_returns) if code == anchor_symbol),
        None
    )

    # 成交额排名（第1名=最高）
    all_amounts_for_rank = dict(core_stock_amounts)  # core_universe 各股
    all_amounts_for_rank[anchor_symbol] = anchor_amount  # 加入铂力特

    sorted_amounts = sorted(all_amounts_for_rank.items(), key=lambda x: x[1], reverse=True)
    amount_rank_in_sector = next(
        (i + 1 for i, (code, _) in enumerate(sorted_amounts) if code == anchor_symbol),
        None
    )

    # sector_total_size = 参与排名的总数（core_universe 有效数 + 铂力特）
    sector_total_size = len(all_returns_for_rank)

    print(f"[INFO] 板块排名（含铂力特，共 {sector_total_size} 只）: "
          f"涨跌幅第 {return_rank_in_sector} 名，成交额第 {amount_rank_in_sector} 名")

    # ─────────────────────────────────────────
    # v2.1 新增：标签
    # ─────────────────────────────────────────

    price_strength_label = compute_price_strength_label(
        anchor_return, relative_strength, return_rank_in_sector, sector_total_size
    )
    volume_strength_label = compute_volume_strength_label(
        amount_vs_5d_avg, amount_20d_high, amount_rank_in_sector, sector_total_size
    )
    overall_signal_label = compute_overall_signal_label(
        price_strength_label, volume_strength_label
    )

    # ─────────────────────────────────────────
    # v2.1 新增：异常信号
    # ─────────────────────────────────────────

    abnormal_signals = compute_abnormal_signals(
        relative_strength=relative_strength,
        amount_vs_5d_avg=amount_vs_5d_avg,
        amount_20d_high=amount_20d_high,
        return_rank_in_sector=return_rank_in_sector,
        amount_rank_in_sector=amount_rank_in_sector
    )

    # ─────────────────────────────────────────
    # v2.4 新增：daily_basic（PE/PB/市值）
    # ─────────────────────────────────────────
    basic_fields = _load_daily_basic(anchor_symbol, latest_date)

    # ─────────────────────────────────────────
    # v2.4 新增：moneyflow（资金流向）
    # ─────────────────────────────────────────
    flow_fields = _load_moneyflow(anchor_symbol, latest_date)

    # ─────────────────────────────────────────
    # v2.5 新增：三类状态标签
    # ─────────────────────────────────────────
    valuation_label = compute_valuation_label(basic_fields.get('pe_ttm'))

    capital_flow_label = compute_capital_flow_label(
        net_mf_amount=flow_fields.get('net_mf_amount'),
        buy_elg_vol=flow_fields.get('buy_elg_vol'),
        sell_elg_vol=flow_fields.get('sell_elg_vol'),
    )

    activity_label = compute_activity_label(
        turnover_rate=basic_fields.get('turnover_rate'),
        amount_vs_5d_avg=amount_vs_5d_avg,
        amount_rank_in_sector=amount_rank_in_sector,
        sector_total_size=sector_total_size,
    )

    print(f"[INFO] 状态标签: 估值={valuation_label} | 资金={capital_flow_label} | 活跃度={activity_label}")

    # ─────────────────────────────────────────
    # v2.5B 新增：资金结构层（单日）
    # ─────────────────────────────────────────
    capital_struct = compute_capital_structure(
        net_mf_amount   = flow_fields.get('net_mf_amount'),
        buy_sm_amount   = flow_fields.get('buy_sm_amount'),
        sell_sm_amount  = flow_fields.get('sell_sm_amount'),
        buy_md_amount   = flow_fields.get('buy_md_amount'),
        sell_md_amount  = flow_fields.get('sell_md_amount'),
        buy_lg_amount   = flow_fields.get('buy_lg_amount'),
        sell_lg_amount  = flow_fields.get('sell_lg_amount'),
        buy_elg_amount  = flow_fields.get('buy_elg_amount'),
        sell_elg_amount = flow_fields.get('sell_elg_amount'),
        anchor_return   = anchor_return,
    )
    print(f"[INFO] 资金结构: {capital_struct['capital_structure_label']} | "
          f"价格资金关系: {capital_struct['price_capital_relation_label']}")

    universe_version = get_universe_version(config)

    # ─────────────────────────────────────────
    # research_core 计算（v2.6 新增）
    # 读取 config 中的 research_core 层，计算平行的研究层相对强弱
    # 原有 trading_core 字段（sector_avg_return / relative_strength）完全不变
    # ─────────────────────────────────────────
    research_avg_return, research_relative_strength = _compute_research_core(
        config=config,
        df=df,
        anchor_symbol=anchor_symbol,
        anchor_return=anchor_return,
        latest_date=latest_date,
    )
    if research_avg_return is not None:
        print(f"[INFO] research_core: avg={research_avg_return:+.4%}  "
              f"research_rs={research_relative_strength:+.4%}")
    else:
        print("[INFO] research_core: 数据不足，跳过")

    result = {
        # 原有字段
        'trade_date': latest_date,
        'anchor_symbol': anchor_symbol,
        'anchor_return': anchor_return,
        'sector_avg_return': sector_avg_return,
        'core_universe_count': participated_count,
        'sector_total_count': core_total_count,
        'relative_strength': relative_strength,
        'anchor_amount': anchor_amount,
        'amount_20d_high': amount_20d_high,
        'amount_vs_5d_avg': amount_vs_5d_avg,
        # v2.1 新增
        'return_rank_in_sector': return_rank_in_sector,
        'amount_rank_in_sector': amount_rank_in_sector,
        'sector_total_size': sector_total_size,
        'price_strength_label': price_strength_label,
        'volume_strength_label': volume_strength_label,
        'overall_signal_label': overall_signal_label,
        'abnormal_signals': abnormal_signals,
        # v2.4 新增：基本面
        **basic_fields,
        # v2.4 新增：资金流向
        **flow_fields,
        # v2.5 新增：状态标签
        'valuation_label':    valuation_label,
        'capital_flow_label': capital_flow_label,
        'activity_label':     activity_label,
        # v2.5B 新增：资金结构层
        'retail_order_net':             capital_struct['retail_order_net'],
        'big_order_ratio':              capital_struct['big_order_ratio'],
        'capital_structure_label':      capital_struct['capital_structure_label'],
        'price_capital_relation_label': capital_struct['price_capital_relation_label'],
        # v2.5A 新增：股票池版本
        'universe_version':   universe_version,
        # v2.6 新增：research_core 平行字段
        'research_avg_return':          research_avg_return,
        'research_relative_strength':   research_relative_strength,
    }

    return result


def compute_valuation_label(pe_ttm: Optional[float]) -> str:
    """
    估值状态标签（v2.5）

    规则（绝对阈值，科创板成长股适用）：
      pe_ttm <= 0 或 None  → 估值暂不可判（亏损或数据缺失）
      pe_ttm > 100         → 高估值
      pe_ttm > 50          → 中性偏高
      pe_ttm > 30          → 中性估值
      pe_ttm > 0           → 低估值

    注：铂力特当前 PE_TTM ~120，属于高估值区间。
    科创板成长股 PE 普遍偏高，阈值设置偏宽松。
    """
    if pe_ttm is None or pe_ttm <= 0:
        return "估值暂不可判"
    if pe_ttm > 100:
        return "高估值"
    if pe_ttm > 50:
        return "中性偏高"
    if pe_ttm > 30:
        return "中性估值"
    return "低估值"


def compute_capital_flow_label(
    net_mf_amount: Optional[float],
    buy_elg_vol: Optional[float],
    sell_elg_vol: Optional[float],
) -> str:
    """
    资金状态标签（v2.5）

    规则：
      两个维度综合判断：
        1. net_mf_amount（主力净流入，万元）
        2. 超大单净量 = buy_elg_vol - sell_elg_vol（手）

      主力偏多：net_mf_amount > 1000 万元 或 超大单净量 > 500 手
      主力偏空：net_mf_amount < -1000 万元 或 超大单净量 < -500 手
      主力中性：其余

      两个维度有冲突时（一正一负），以 net_mf_amount 为主。
      任一维度缺失时，用另一维度单独判断。
      两者均缺失时，返回"资金数据不可用"。
    """
    if net_mf_amount is None and (buy_elg_vol is None or sell_elg_vol is None):
        return "资金数据不可用"

    net_elg = None
    if buy_elg_vol is not None and sell_elg_vol is not None:
        net_elg = buy_elg_vol - sell_elg_vol

    # 以 net_mf_amount 为主判断
    if net_mf_amount is not None:
        if net_mf_amount > 1000:
            return "主力偏多"
        if net_mf_amount < -1000:
            return "主力偏空"
        # net_mf_amount 中性区间，用超大单辅助
        if net_elg is not None:
            if net_elg > 500:
                return "主力偏多"
            if net_elg < -500:
                return "主力偏空"
        return "主力中性"

    # net_mf_amount 缺失，只用超大单
    if net_elg is not None:
        if net_elg > 500:
            return "主力偏多"
        if net_elg < -500:
            return "主力偏空"
        return "主力中性"

    return "资金数据不可用"


def compute_capital_structure(
    net_mf_amount: Optional[float],
    buy_sm_amount: Optional[float],
    sell_sm_amount: Optional[float],
    buy_md_amount: Optional[float],
    sell_md_amount: Optional[float],
    buy_lg_amount: Optional[float],
    sell_lg_amount: Optional[float],
    buy_elg_amount: Optional[float],
    sell_elg_amount: Optional[float],
    anchor_return: float,
) -> Dict[str, Any]:
    """
    v2.5B 资金结构层（单日）

    返回字段：
      retail_order_net        中小资金净流入额（万元）
      big_order_ratio         大资金双边成交额占比（0~1）
      capital_structure_label 资金主导结构标签
      price_capital_relation_label 价格资金关系标签
    """
    empty = {
        'retail_order_net': None,
        'big_order_ratio': None,
        'capital_structure_label': '资金结构数据不可用',
        'price_capital_relation_label': '资金结构数据不可用',
    }

    # 任何一个核心字段缺失就降级
    required = [buy_sm_amount, sell_sm_amount, buy_md_amount, sell_md_amount,
                buy_lg_amount, sell_lg_amount, buy_elg_amount, sell_elg_amount]
    if any(v is None for v in required):
        return empty

    # ── 中间量计算 ──────────────────────────────────────────
    retail_order_net = (buy_sm_amount - sell_sm_amount) + (buy_md_amount - sell_md_amount)

    big_bilateral = buy_lg_amount + sell_lg_amount + buy_elg_amount + sell_elg_amount
    total_bilateral = (buy_sm_amount + sell_sm_amount + buy_md_amount + sell_md_amount
                       + buy_lg_amount + sell_lg_amount + buy_elg_amount + sell_elg_amount)
    big_order_ratio = big_bilateral / total_bilateral if total_bilateral > 0 else None

    # ── capital_structure_label ──────────────────────────────
    # 大资金净流入（net_mf_amount = 大单净额 + 超大单净额）
    big_net = net_mf_amount  # 已有，直接复用

    # 判断大资金方向
    big_bullish  = big_net is not None and big_net > 1000
    big_bearish  = big_net is not None and big_net < -1000

    # 判断中小资金方向（显著：绝对值 > 500万）
    retail_bullish = retail_order_net > 500
    retail_bearish = retail_order_net < -500

    # 大资金占比高低
    big_dominant   = big_order_ratio is not None and big_order_ratio > 0.40
    retail_dominant = big_order_ratio is not None and big_order_ratio < 0.30

    if big_bullish and big_dominant:
        capital_structure_label = "大资金主导买入"
    elif big_bearish and big_dominant:
        capital_structure_label = "大资金主导卖出"
    elif retail_dominant and (retail_bullish or retail_bearish):
        # 中小资金主导：占比低 + 中小资金有显著方向
        capital_structure_label = "中小资金主导"
    elif (big_bullish and retail_bearish) or (big_bearish and retail_bullish):
        capital_structure_label = "资金分歧"
    else:
        capital_structure_label = "方向不明"

    # ── price_capital_relation_label ────────────────────────
    # 阈值：价格波动 > 1% 视为有方向；资金净流入绝对值 > 200万视为显著
    price_up   = anchor_return > 0.01
    price_down = anchor_return < -0.01
    price_flat = not price_up and not price_down

    big_net_in  = big_net is not None and big_net > 200
    big_net_out = big_net is not None and big_net < -200

    if price_flat or (not big_net_in and not big_net_out):
        price_capital_relation_label = "中性"
    elif price_up and big_net_in:
        price_capital_relation_label = "上行配合"
    elif price_down and big_net_out:
        price_capital_relation_label = "下行配合"
    elif price_up and big_net_out:
        price_capital_relation_label = "上涨背离"
    elif price_down and big_net_in:
        price_capital_relation_label = "下跌背离"
    else:
        price_capital_relation_label = "中性"

    return {
        'retail_order_net':             retail_order_net,
        'big_order_ratio':              big_order_ratio,
        'capital_structure_label':      capital_structure_label,
        'price_capital_relation_label': price_capital_relation_label,
    }


def compute_activity_label(
    turnover_rate: Optional[float],
    amount_vs_5d_avg: Optional[float],
    amount_rank_in_sector: Optional[int],
    sector_total_size: int,
) -> str:
    """
    活跃度标签（v2.5）

    规则：
      活跃：turnover_rate > 3% 或 amount_vs_5d_avg > 1.5
      低活跃：turnover_rate < 1% 且 amount_vs_5d_avg < 0.7（两条均满足）
      正常：其余

      turnover_rate 缺失时，只用 amount_vs_5d_avg 判断。
      两者均缺失时，返回"活跃度数据不可用"。
    """
    if turnover_rate is None and amount_vs_5d_avg is None:
        return "活跃度数据不可用"

    # 活跃判断（任一满足）
    is_active = (
        (turnover_rate is not None and turnover_rate > 3) or
        (amount_vs_5d_avg is not None and amount_vs_5d_avg > 1.5)
    )
    if is_active:
        return "活跃"

    # 低活跃判断（两条均满足，有缺失时只用可用的）
    low_turnover = (turnover_rate is not None and turnover_rate < 1)
    low_amount   = (amount_vs_5d_avg is not None and amount_vs_5d_avg < 0.7)

    if turnover_rate is None:
        # 只有 amount 数据
        if low_amount:
            return "低活跃"
    elif amount_vs_5d_avg is None:
        # 只有换手率数据
        if low_turnover:
            return "低活跃"
    else:
        # 两者都有，需同时满足
        if low_turnover and low_amount:
            return "低活跃"

    return "正常"


def _load_daily_basic(anchor_symbol: str, trade_date) -> dict:
    """
    从 raw 层加载 daily_basic，提取当日 PE/PB/市值字段。
    文件不存在或当日无数据时返回空字段（不影响主流程）。
    """
    path = DATA_RAW_DIR / "daily_basic.parquet"
    empty = {
        'pe_ttm': None, 'pb': None,
        'total_mv': None, 'circ_mv': None,
        'turnover_rate': None, 'turnover_rate_f': None,
    }
    if not path.exists():
        return empty
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return empty
        # trade_date 统一格式
        if df['trade_date'].dtype.kind == 'M':
            df['trade_date_str'] = df['trade_date'].dt.strftime('%Y%m%d')
        else:
            df['trade_date_str'] = df['trade_date'].astype(str)

        target_date = pd.Timestamp(trade_date).strftime('%Y%m%d') if hasattr(trade_date, 'strftime') else str(trade_date).replace('-', '')[:8]
        row = df[(df['ts_code'] == anchor_symbol) & (df['trade_date_str'] == target_date)]
        if row.empty:
            return empty
        r = row.iloc[0]
        result = {
            'pe_ttm':         float(r['pe_ttm'])         if 'pe_ttm'         in r and pd.notna(r['pe_ttm'])         else None,
            'pb':             float(r['pb'])              if 'pb'             in r and pd.notna(r['pb'])             else None,
            'total_mv':       float(r['total_mv'])        if 'total_mv'       in r and pd.notna(r['total_mv'])       else None,
            'circ_mv':        float(r['circ_mv'])         if 'circ_mv'        in r and pd.notna(r['circ_mv'])        else None,
            'turnover_rate':  float(r['turnover_rate'])   if 'turnover_rate'  in r and pd.notna(r['turnover_rate'])  else None,
            'turnover_rate_f':float(r['turnover_rate_f']) if 'turnover_rate_f' in r and pd.notna(r['turnover_rate_f']) else None,
        }
        print(f"[INFO] daily_basic: PE_TTM={result['pe_ttm']}, PB={result['pb']}, "
              f"总市值={result['total_mv']}万元, 换手率={result['turnover_rate']}%")
        return result
    except Exception as e:
        print(f"[WARN] daily_basic 加载失败（不影响主流程）: {e}")
        return empty


def _load_moneyflow(anchor_symbol: str, trade_date) -> dict:
    """
    从 raw 层加载 moneyflow，提取当日净流入字段。
    文件不存在或当日无数据时返回空字段（不影响主流程）。
    """
    path = DATA_RAW_DIR / "moneyflow.parquet"
    empty = {
        'net_mf_amount': None,
        'buy_elg_vol': None, 'sell_elg_vol': None,
        'buy_lg_vol': None,  'sell_lg_vol': None,
        # v2.5B 新增：各层级 amount 字段
        'buy_sm_amount': None, 'sell_sm_amount': None,
        'buy_md_amount': None, 'sell_md_amount': None,
        'buy_lg_amount': None, 'sell_lg_amount': None,
        'buy_elg_amount': None, 'sell_elg_amount': None,
    }
    if not path.exists():
        return empty
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return empty
        if df['trade_date'].dtype.kind == 'M':
            df['trade_date_str'] = df['trade_date'].dt.strftime('%Y%m%d')
        else:
            df['trade_date_str'] = df['trade_date'].astype(str)

        target_date = pd.Timestamp(trade_date).strftime('%Y%m%d') if hasattr(trade_date, 'strftime') else str(trade_date).replace('-', '')[:8]
        row = df[(df['ts_code'] == anchor_symbol) & (df['trade_date_str'] == target_date)]
        if row.empty:
            return empty
        r = row.iloc[0]
        def _f(col):
            return float(r[col]) if col in r.index and pd.notna(r[col]) else None

        result = {
            'net_mf_amount':  _f('net_mf_amount'),
            'buy_elg_vol':    _f('buy_elg_vol'),
            'sell_elg_vol':   _f('sell_elg_vol'),
            'buy_lg_vol':     _f('buy_lg_vol'),
            'sell_lg_vol':    _f('sell_lg_vol'),
            # v2.5B 新增
            'buy_sm_amount':  _f('buy_sm_amount'),
            'sell_sm_amount': _f('sell_sm_amount'),
            'buy_md_amount':  _f('buy_md_amount'),
            'sell_md_amount': _f('sell_md_amount'),
            'buy_lg_amount':  _f('buy_lg_amount'),
            'sell_lg_amount': _f('sell_lg_amount'),
            'buy_elg_amount': _f('buy_elg_amount'),
            'sell_elg_amount':_f('sell_elg_amount'),
        }
        net = result['net_mf_amount']
        print(f"[INFO] moneyflow: 净流入={net:.0f}万元" if net is not None else "[INFO] moneyflow: 净流入=N/A")
        return result
    except Exception as e:
        print(f"[WARN] moneyflow 加载失败（不影响主流程）: {e}")
        return empty


def _compute_research_core(
    config: Dict[str, Any],
    df: pd.DataFrame,
    anchor_symbol: str,
    anchor_return: float,
    latest_date,
) -> tuple:
    """
    计算 research_core 层的平均涨跌幅和相对强弱（v2.6）

    读取 config['research_core']，计算：
      - research_avg_return: research_core 成员当日平均涨跌幅
      - research_relative_strength: anchor_return - research_avg_return

    设计原则：
      - 与 trading_core 计算完全平行，互不干扰
      - research_core 成员与 anchor 重叠时自动排除（如航天动力同时在两层）
      - 任一成员数据缺失时静默跳过，不影响主流程
      - 覆盖度 < 50% 时返回 (None, None)，不产生无意义结论

    Returns:
        (research_avg_return, research_relative_strength)
        数据不足时返回 (None, None)
    """
    research_universe = config.get('research_core', [])
    if not research_universe:
        return None, None

    research_codes = [
        s['code'] for s in research_universe
        if s.get('active', True) and s.get('code') and s['code'] != anchor_symbol
    ]
    if not research_codes:
        return None, None

    research_returns = []
    missing = []

    for code in research_codes:
        stock_df = df[df['ts_code'] == code].copy()
        stock_df = stock_df.sort_values('trade_date').reset_index(drop=True)

        latest_row = stock_df[stock_df['trade_date'] == latest_date]
        if latest_row.empty:
            missing.append(code)
            continue

        latest_idx = stock_df[stock_df['trade_date'] == latest_date].index[0]
        if latest_idx == 0:
            missing.append(code)
            continue

        prev_close = stock_df.iloc[latest_idx - 1]['close']
        curr_close = latest_row.iloc[0]['close']
        if prev_close > 0:
            research_returns.append((curr_close - prev_close) / prev_close)
        else:
            missing.append(code)

    total = len(research_codes)
    participated = len(research_returns)
    coverage = participated / total if total > 0 else 0

    if missing:
        print(f"[INFO] research_core: {participated}/{total} 只参与，缺失: {missing}")

    if coverage < 0.5:
        print(f"[WARN] research_core: 覆盖度不足 ({participated}/{total})，跳过计算")
        return None, None

    research_avg = sum(research_returns) / len(research_returns)
    research_rs = anchor_return - research_avg
    return research_avg, research_rs


def save_metrics(result: Dict[str, Any], output_path: str = None):
    """保存指标结果到 parquet 文件"""
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH

    output_file = Path(output_path)

    if not result:
        raise ValueError("指标结果为空，无法保存")

    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        # abnormal_signals 是 list，parquet 支持，直接保存
        df = pd.DataFrame([result])
        df.to_parquet(output_file, index=False)
        print(f"[INFO] 指标已保存至: {output_file}")
    except Exception as e:
        raise ValueError(f"保存指标失败: {e}")


def main():
    """主函数：分析锚定标的并保存结果"""
    print("=" * 50)
    print("开始分析锚定标的指标（v2.1）...")
    print("=" * 50)

    result = analyze_anchor_symbol()

    print(f"\n分析日期: {result['trade_date'].strftime('%Y-%m-%d')}")
    print(f"锚定标的: {result['anchor_symbol']}")
    print(f"\n--- 涨跌幅分析 ---")
    print(f"锚定标的涨跌幅: {result['anchor_return']:.2%}")
    print(f"板块平均涨跌幅（core_universe）: {result['sector_avg_return']:.2%}")
    print(f"参与计算股票数（core_universe）: {result['core_universe_count']}/{result['sector_total_count']}")
    print(f"相对板块强弱: {result['relative_strength']:.2%}")
    print(f"\n--- 成交额分析 ---")
    print(f"锚定标的成交额: {result['anchor_amount']:,.2f} 万元")
    print(f"创20日新高: {'是' if result['amount_20d_high'] else '否'}")
    print(f"相对5日均值: {result['amount_vs_5d_avg']:.2f} 倍" if result['amount_vs_5d_avg'] else "相对5日均值: N/A")
    print(f"\n--- 板块排名（含铂力特，共 {result['sector_total_size']} 只）---")
    print(f"涨跌幅排名: 第 {result['return_rank_in_sector']} 名")
    print(f"成交额排名: 第 {result['amount_rank_in_sector']} 名")
    print(f"\n--- 标签 ---")
    print(f"价格强度: {result['price_strength_label']}")
    print(f"成交额强度: {result['volume_strength_label']}")
    print(f"综合信号: {result['overall_signal_label']}")
    print(f"\n--- 异常信号 ---")
    if result['abnormal_signals']:
        for sig in result['abnormal_signals']:
            print(f"  ⚡ {sig}")
    else:
        print("  无异常信号")

    save_metrics(result)
    return result


if __name__ == "__main__":
    main()
