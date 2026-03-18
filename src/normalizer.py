"""
数据标准化模块 v2.3
将 raw 层数据转换为 normalized 层

职责（只做格式统一，不做业务计算）：
  - ts_code：统一为含交易所后缀格式（688333 → 688333.SH）
  - trade_date：统一为 datetime64[ns]
  - amount：保持千元原始值，增加 amount_unit 标注列
  - vol：保持手为单位，增加 vol_unit 标注列
  - 增加 data_source 字段
  - 过滤无效行（close=0 或 null）

不做：
  - 单位转换（amount 保持千元）
  - 业务指标计算
  - 复权处理

输出：data/normalized/market_data_normalized.parquet
"""

import pandas as pd
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_NORMALIZED_DIR = PROJECT_ROOT / "data" / "normalized"

DEFAULT_INPUT_PATH = DATA_RAW_DIR / "market_data.parquet"
DEFAULT_OUTPUT_PATH = DATA_NORMALIZED_DIR / "market_data_normalized.parquet"

# 交易所后缀映射（按股票代码前缀判断）
# 6xxxxx → SH（上交所）
# 0xxxxx / 3xxxxx → SZ（深交所）
_EXCHANGE_MAP = {
    "6": "SH",
    "0": "SZ",
    "3": "SZ",
    "4": "BJ",
    "8": "BJ",
}


def _infer_exchange(code: str) -> str:
    """根据股票代码前缀推断交易所后缀"""
    pure = str(code).split(".")[0].strip()
    prefix = pure[0] if pure else ""
    return _EXCHANGE_MAP.get(prefix, "SH")


def normalize(
    input_path: str = None,
    output_path: str = None,
    data_source: str = "tushare"
) -> pd.DataFrame:
    """
    标准化 raw 层数据，输出 normalized 层

    Args:
        input_path:  raw 数据路径，默认 data/raw/market_data.parquet
        output_path: 输出路径，默认 data/normalized/market_data_normalized.parquet
        data_source: 数据来源标记，默认 tushare

    Returns:
        标准化后的 DataFrame

    Raises:
        FileNotFoundError: raw 数据文件不存在
        ValueError: 数据格式异常
    """
    if input_path is None:
        input_path = DEFAULT_INPUT_PATH
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH

    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"raw 数据文件不存在: {input_file}")

    df = pd.read_parquet(input_file)
    if df.empty:
        raise ValueError(f"raw 数据文件为空: {input_file}")

    original_count = len(df)

    # ── 1. ts_code 统一为含交易所后缀格式 ──
    # Tushare 返回的已经是 688333.SH 格式，但做一次防御性处理
    def _normalize_code(code: str) -> str:
        code = str(code).strip()
        if "." in code:
            return code.upper()
        exchange = _infer_exchange(code)
        return f"{code}.{exchange}"

    df["ts_code"] = df["ts_code"].apply(_normalize_code)

    # ── 2. trade_date 统一为 datetime64[ns] ──
    if df["trade_date"].dtype == object or df["trade_date"].dtype.kind in ("U", "S"):
        # YYYYMMDD 字符串 → datetime
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
    elif df["trade_date"].dtype.kind != "M":
        # 其他非日期类型，尝试通用解析
        df["trade_date"] = pd.to_datetime(df["trade_date"])
    # 已经是 datetime64 则不做处理

    # ── 3. 过滤无效行 ──
    # close = 0 或 null 的行视为无效数据
    before_filter = len(df)
    df = df[df["close"].notna() & (df["close"] > 0)]
    filtered = before_filter - len(df)
    if filtered > 0:
        print(f"[WARN] normalizer: 过滤 {filtered} 条无效行（close=0 或 null）")

    # ── 4. 增加元数据字段 ──
    # amount_unit：明确标注 amount 字段的单位，防止误用
    df["amount_unit"] = "千元"   # ⚠️ Tushare amount 原始单位，不做转换

    # vol_unit：明确标注 vol 字段的单位
    df["vol_unit"] = "手"        # 1手 = 100股

    # data_source：标记数据来源
    df["data_source"] = data_source

    # ── 5. 列排序（便于阅读）──
    base_cols = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
    meta_cols = ["amount_unit", "vol_unit", "data_source"]
    other_cols = [c for c in df.columns if c not in base_cols + meta_cols]
    df = df[base_cols + other_cols + meta_cols]

    # ── 6. 保存 ──
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_file, index=False)

    print(f"[INFO] normalizer: {original_count} 条 → {len(df)} 条（过滤 {original_count - len(df)} 条）")
    print(f"[INFO] normalizer: 输出至 {output_file}")
    print(f"[INFO] normalizer: amount 单位 = 千元（Tushare 原始，未转换）")

    return df


def load_normalized(path: str = None) -> pd.DataFrame:
    """
    加载 normalized 层数据（供 analyzer 使用）

    如果 normalized 文件不存在，自动从 raw 层生成。
    """
    if path is None:
        path = DEFAULT_OUTPUT_PATH

    norm_file = Path(path)

    if not norm_file.exists():
        print(f"[INFO] normalizer: normalized 文件不存在，从 raw 层自动生成...")
        return normalize()

    df = pd.read_parquet(norm_file)

    # 确保 trade_date 是 datetime 类型（parquet 读取后应该已经是）
    if df["trade_date"].dtype.kind != "M":
        df["trade_date"] = pd.to_datetime(df["trade_date"])

    return df


def main():
    """独立运行标准化（调试用）"""
    print("=" * 50)
    print("数据标准化（normalizer v2.3）")
    print("=" * 50)
    df = normalize()
    print(f"\n字段列表: {df.columns.tolist()}")
    print(f"数据样例:")
    print(df[["ts_code", "trade_date", "close", "amount", "amount_unit"]].tail(3).to_string())


if __name__ == "__main__":
    main()
