"""
价格数据产品模块 v1.0

职责：
  - 统一价格链路的数据产品入口
  - 读取 market_data / daily_basic / moneyflow
  - 产出 data/processed/price_data_product.json
  - 为 analyzer / reporter 提供稳定加载接口
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "stocks.yaml"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
NORMALIZED_DIR = PROJECT_ROOT / "data" / "normalized"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

MARKET_NORMALIZED_PATH = NORMALIZED_DIR / "market_data_normalized.parquet"
MARKET_RAW_PATH = RAW_DIR / "market_data.parquet"
DAILY_BASIC_PATH = RAW_DIR / "daily_basic.parquet"
MONEYFLOW_PATH = RAW_DIR / "moneyflow.parquet"
DEFAULT_OUTPUT_PATH = PROCESSED_DIR / "price_data_product.json"


def load_config(config_path: str = None) -> Dict[str, Any]:
    path = Path(config_path) if config_path else CONFIG_PATH
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _to_trade_date_str(value) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return pd.Timestamp(value).strftime("%Y%m%d")


def _normalize_trade_date(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if df["trade_date"].dtype.kind != "M":
        try:
            df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
        except Exception:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df


def _safe_read_parquet(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "trade_date" in df.columns:
        df = _normalize_trade_date(df)
    return df


def _load_market_data_with_status() -> Dict[str, Any]:
    source_path = None
    source_name = None

    if MARKET_NORMALIZED_PATH.exists():
        source_path = MARKET_NORMALIZED_PATH
        source_name = "normalized"
    elif MARKET_RAW_PATH.exists():
        source_path = MARKET_RAW_PATH
        source_name = "raw"

    if source_path is None:
        return {
            "status": "error",
            "reason": f"市场数据文件不存在: {MARKET_NORMALIZED_PATH.name} / {MARKET_RAW_PATH.name}",
            "source": None,
            "data": None,
            "record_count": 0,
            "latest_trade_date": None,
        }

    try:
        df = _safe_read_parquet(source_path)
    except Exception as e:
        return {
            "status": "error",
            "reason": f"读取市场数据失败: {e}",
            "source": source_name,
            "data": None,
            "record_count": 0,
            "latest_trade_date": None,
        }

    required_cols = ["ts_code", "trade_date", "close", "amount"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if df.empty or missing_cols:
        reason = "市场数据为空" if df.empty else f"市场数据缺少必要列: {missing_cols}"
        return {
            "status": "error",
            "reason": reason,
            "source": source_name,
            "data": df,
            "record_count": len(df),
            "latest_trade_date": None,
        }

    latest_trade_date = df["trade_date"].max()
    return {
        "status": "ok",
        "reason": None,
        "source": source_name,
        "data": df,
        "record_count": len(df),
        "latest_trade_date": _to_trade_date_str(latest_trade_date),
    }


def _load_optional_anchor_dataset(path: Path, anchor_symbol: str, latest_trade_date: Optional[str]) -> Dict[str, Any]:
    if not path.exists():
        return {
            "status": "empty",
            "reason": f"{path.name} 不存在",
            "record_count": 0,
            "latest_trade_date": latest_trade_date,
            "data": None,
        }

    try:
        df = _safe_read_parquet(path)
    except Exception as e:
        return {
            "status": "error",
            "reason": f"读取 {path.name} 失败: {e}",
            "record_count": 0,
            "latest_trade_date": latest_trade_date,
            "data": None,
        }

    if df.empty:
        return {
            "status": "empty",
            "reason": f"{path.name} 为空",
            "record_count": 0,
            "latest_trade_date": latest_trade_date,
            "data": df,
        }

    if "trade_date" in df.columns:
        trade_date_str = df["trade_date"].map(_to_trade_date_str)
    else:
        trade_date_str = pd.Series([None] * len(df))

    anchor_mask = df["ts_code"] == anchor_symbol if "ts_code" in df.columns else pd.Series([False] * len(df))
    date_mask = trade_date_str == latest_trade_date if latest_trade_date else pd.Series([True] * len(df))
    anchor_df = df[anchor_mask & date_mask].copy()

    status = "ok" if not anchor_df.empty else "empty"
    reason = None if status == "ok" else f"{path.name} 无 {anchor_symbol} 在 {latest_trade_date} 的记录"
    return {
        "status": status,
        "reason": reason,
        "record_count": len(df),
        "latest_trade_date": latest_trade_date,
        "data": df,
    }


def _derive_overall_status(market_status: str, basic_status: str, moneyflow_status: str) -> str:
    if market_status != "ok":
        return "error"
    if basic_status == "ok" and moneyflow_status == "ok":
        return "ok"
    return "partial"


def build_price_data_product(config_path: str = None, output_path: str = None) -> Dict[str, Any]:
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH

    config = load_config(config_path)
    anchor_symbol = config["anchor"]["code"]

    market = _load_market_data_with_status()
    latest_trade_date = market["latest_trade_date"]
    daily_basic = _load_optional_anchor_dataset(DAILY_BASIC_PATH, anchor_symbol, latest_trade_date)
    moneyflow = _load_optional_anchor_dataset(MONEYFLOW_PATH, anchor_symbol, latest_trade_date)

    product = {
        "anchor_symbol": anchor_symbol,
        "latest_trade_date": latest_trade_date,
        "market_data_status": market["status"],
        "market_data_reason": market["reason"],
        "market_data_source": market["source"],
        "market_record_count": market["record_count"],
        "daily_basic_status": daily_basic["status"],
        "daily_basic_reason": daily_basic["reason"],
        "daily_basic_record_count": daily_basic["record_count"],
        "moneyflow_status": moneyflow["status"],
        "moneyflow_reason": moneyflow["reason"],
        "moneyflow_record_count": moneyflow["record_count"],
        "overall_status": _derive_overall_status(
            market["status"],
            daily_basic["status"],
            moneyflow["status"],
        ),
        "generated_at": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False, indent=2)

    return product


def load_price_data_product(path: str = None) -> Dict[str, Any]:
    product_path = Path(path) if path else DEFAULT_OUTPUT_PATH
    if not product_path.exists():
        raise FileNotFoundError(f"价格数据产品不存在: {product_path}")

    with open(product_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_price_inputs(config_path: str = None, product_path: str = None) -> Dict[str, Any]:
    try:
        product = load_price_data_product(product_path)
    except FileNotFoundError:
        product = build_price_data_product(config_path=config_path, output_path=product_path)

    if product["market_data_status"] != "ok":
        raise ValueError(f"价格底座不可用: {product.get('market_data_reason') or product['market_data_status']}")

    market = _load_market_data_with_status()
    if market["status"] != "ok" or market["data"] is None:
        raise ValueError(f"价格底座不可用: {market.get('reason') or market['status']}")

    anchor_symbol = product["anchor_symbol"]
    latest_trade_date = product["latest_trade_date"]
    daily_basic = _load_optional_anchor_dataset(DAILY_BASIC_PATH, anchor_symbol, latest_trade_date)
    moneyflow = _load_optional_anchor_dataset(MONEYFLOW_PATH, anchor_symbol, latest_trade_date)

    return {
        "product": product,
        "market_data": market["data"],
        "daily_basic_data": daily_basic["data"],
        "moneyflow_data": moneyflow["data"],
    }
