"""
数据存储模块
管理原始数据和处理后数据的文件存储
"""
from pathlib import Path
from typing import Optional

import pandas as pd

# 项目根目录（基于本文件位置计算）
PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"


def save_market_data(
    df: pd.DataFrame,
    output_path: str = None
) -> None:
    """
    保存原始日线数据到 parquet 文件

    Args:
        df: 包含日线数据的 DataFrame
        output_path: 输出文件路径，默认使用 data/raw/market_data.parquet

    Raises:
        ValueError: 数据为空或无法写入
    """
    if output_path is None:
        output_path = RAW_DIR / "market_data.parquet"

    output_file = Path(output_path)

    if df is None or df.empty:
        raise ValueError("数据为空，无法保存")

    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_file, index=False)
        print(f"[INFO] 数据已保存至 {output_file}")
    except Exception as e:
        raise ValueError(f"保存数据失败: {e}")


def load_market_data(
    input_path: str = None
) -> pd.DataFrame:
    """
    加载原始日线数据

    Args:
        input_path: 输入文件路径，默认使用 data/raw/market_data.parquet

    Returns:
        DataFrame

    Raises:
        FileNotFoundError: 文件不存在
        ValueError: 文件格式错误或为空
    """
    if input_path is None:
        input_path = RAW_DIR / "market_data.parquet"

    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"数据文件不存在: {input_file}")

    try:
        df = pd.read_parquet(input_file)
    except Exception as e:
        raise ValueError(f"读取数据失败: {e}")

    if df.empty:
        raise ValueError(f"数据文件为空: {input_file}")

    return df


def verify_data_completeness(
    df: pd.DataFrame,
    expected_codes: list[str]
) -> dict:
    """
    验证数据完整性

    Args:
        df: 日线数据 DataFrame
        expected_codes: 预期的股票代码列表

    Returns:
        验证结果字典，包含:
        - total_records: 总记录数
        - fetched_codes: 已获取的股票代码
        - missing_codes: 缺失的股票代码
        - is_complete: 是否完整
    """
    fetched_codes = set(df["ts_code"].unique())
    missing_codes = set(expected_codes) - fetched_codes

    return {
        "total_records": len(df),
        "fetched_codes": list(fetched_codes),
        "missing_codes": list(missing_codes),
        "is_complete": len(missing_codes) == 0
    }


if __name__ == "__main__":
    # 测试加载
    df = load_market_data()
    if df is not None:
        print("数据概览:")
        print(df.head())
        print(f"\n列名: {df.columns.tolist()}")
        print(f"股票数量: {df['ts_code'].nunique()}")
        print(f"日期范围: {df['trade_date'].min()} ~ {df['trade_date'].max()}")