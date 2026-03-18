"""
数据获取模块
使用 Tushare 获取股票市场数据

适配 120 积分账号限制：
- 使用 daily 接口（120 积分可用）
- 逐只股票查询，避免批量限制
- 请求间隔 0.5 秒，避免频次限制
- 简单重试机制，提高稳定性
"""

import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from dotenv import load_dotenv
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from tushare_proxy import pro_api as _proxy_pro_api


# 项目根目录（基于本文件位置计算）
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"

# 默认路径
DEFAULT_CONFIG_PATH = CONFIG_DIR / "stocks.yaml"
DEFAULT_OUTPUT_PATH = DATA_RAW_DIR / "market_data.parquet"

# 120 积分账号的请求间隔（秒）
# 保守设置，避免触发频次限制
REQUEST_INTERVAL = 0.5

# 最大重试次数
MAX_RETRIES = 3

# 重试间隔基数（指数退避）
RETRY_BASE_DELAY = 1.0


def get_tushare_token() -> str:
    """
    获取 Tushare Token

    优先级：
    1. 环境变量 TUSHARE_TOKEN
    2. .env 文件中的 TUSHARE_TOKEN

    Returns:
        Tushare Token 字符串

    Raises:
        ValueError: 未找到 Token 时抛出
    """
    # 先尝试从 .env 加载
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    token = os.getenv("TUSHARE_TOKEN")
    if not token or token == "your_tushare_token_here":
        raise ValueError(
            "未配置 TUSHARE_TOKEN。请:\n"
            "1. 复制 .env.example 为 .env\n"
            "2. 在 .env 中填入你的 Tushare Token\n"
            "   获取方式：注册 https://tushare.pro 后在个人中心获取"
        )
    return token


def load_config(config_path: str = None) -> dict:
    """加载配置文件

    Args:
        config_path: 配置文件路径，默认使用项目内的 config/stocks.yaml

    Raises:
        FileNotFoundError: 配置文件不存在
        ValueError: 配置文件格式错误
    """
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH

    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_file}")

    try:
        with open(config_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"配置文件格式错误: {e}")


def extract_stock_codes(config: dict) -> list[dict]:
    """
    从配置中提取股票代码列表

    兼容 v2.3 资产化结构（anchor / core_universe / extended_universe）
    和旧版结构（commercial_space_universe）

    Returns:
        list of dict: [{"ts_code": "688393.SH", "name": "铂力特"}, ...]
    """
    stocks = []

    # v2.3 结构：anchor + core_universe + extended_universe
    if "anchor" in config or "core_universe" in config:
        # anchor
        anchor = config.get("anchor", {})
        if anchor.get("code"):
            stocks.append({"ts_code": anchor["code"], "name": anchor.get("name", "")})

        # core_universe
        for item in config.get("core_universe", []):
            if item.get("active", True) and item.get("code"):
                stocks.append({"ts_code": item["code"], "name": item.get("name", "")})

        # extended_universe
        for item in config.get("extended_universe", []):
            if item.get("active", True) and item.get("code"):
                stocks.append({"ts_code": item["code"], "name": item.get("name", "")})

        return stocks

    # 旧版结构（兼容）
    for item in config.get("commercial_space_universe", []):
        stocks.append({
            "ts_code": item["code"],
            "name": item["name"]
        })
    return stocks


def init_tushare():
    """
    初始化代理 API（透明替代 tushare.pro_api）
    业务代码无需感知，调用方式完全一致。
    """
    return _proxy_pro_api()


def fetch_single_stock_daily(
    pro,
    ts_code: str,
    start_date: str,
    end_date: str,
    retries: int = MAX_RETRIES
) -> Optional[pd.DataFrame]:
    """
    获取单只股票的日线数据（带重试机制）

    Args:
        pro: Tushare Pro API 实例
        ts_code: 单只股票代码，如 "688393.SH"
        start_date: 开始日期，格式 "YYYYMMDD"
        end_date: 结束日期，格式 "YYYYMMDD"
        retries: 最大重试次数

    Returns:
        DataFrame 或 None
    """
    for attempt in range(retries):
        try:
            df = pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )

            if df is not None and not df.empty:
                return df
            return None

        except Exception as e:
            error_msg = str(e).lower()

            # 检查是否是频次限制错误
            if "limit" in error_msg or "频繁" in error_msg or "超过" in error_msg:
                wait_time = RETRY_BASE_DELAY * (2 ** attempt)
                print(f"[WARN] 触发频次限制，等待 {wait_time:.1f}s 后重试 ({attempt + 1}/{retries})")
                time.sleep(wait_time)
            else:
                # 其他错误，简单重试
                if attempt < retries - 1:
                    time.sleep(RETRY_BASE_DELAY)
                else:
                    print(f"[ERROR] 获取 {ts_code} 数据失败: {e}")
                    return None

    return None


def fetch_daily_data(
    pro,
    ts_codes: list[str],
    start_date: str,
    end_date: str
) -> Optional[pd.DataFrame]:
    """
    逐只股票获取日线数据（适配 120 积分账号）

    注意：120 积分账号批量查询可能有限制，
    因此采用逐只查询 + 节流的方式，更稳定。

    Args:
        pro: Tushare Pro API 实例
        ts_codes: 股票代码列表，格式如 ["688393.SH", "600118.SH"]
        start_date: 开始日期，格式 "YYYYMMDD"
        end_date: 结束日期，格式 "YYYYMMDD"

    Returns:
        DataFrame 包含 ts_code, trade_date, open, high, low, close, vol, amount
    """
    all_dfs = []
    required_cols = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]

    total = len(ts_codes)
    success_count = 0
    fail_count = 0

    for i, ts_code in enumerate(ts_codes):
        print(f"[INFO] 获取 {ts_code} ({i + 1}/{total})...")

        df = fetch_single_stock_daily(pro, ts_code, start_date, end_date)

        if df is not None and not df.empty:
            # 选取需要的列
            df = df[[c for c in required_cols if c in df.columns]]
            all_dfs.append(df)
            success_count += 1
        else:
            fail_count += 1

        # 节流：每次请求后等待，避免触发频次限制
        # 最后一只股票不需要等待
        if i < total - 1:
            time.sleep(REQUEST_INTERVAL)

    if not all_dfs:
        print("[ERROR] 未获取到任何数据")
        return None

    # 合并所有数据
    result = pd.concat(all_dfs, ignore_index=True)
    print(f"\n[OK] 成功: {success_count}, 失败: {fail_count}")

    return result


def fetch_market_data(
    config_path: str = None,
    output_path: str = None,
    days: int = 60
) -> pd.DataFrame:
    """
    根据 config/stocks.yaml 获取股票池日线数据并保存

    Args:
        config_path: 配置文件路径，默认使用项目内的 config/stocks.yaml
        output_path: 输出 parquet 文件路径，默认使用 data/raw/market_data.parquet
        days: 回溯天数

    Returns:
        合并后的 DataFrame

    Raises:
        FileNotFoundError: 配置文件不存在
        ValueError: 配置文件格式错误或数据获取失败
    """
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH
    # 1. 初始化 Tushare
    print("[INFO] 初始化 Tushare API...")
    pro = init_tushare()

    # 2. 读取配置
    config = load_config(config_path)
    stocks = extract_stock_codes(config)
    ts_codes = [s["ts_code"] for s in stocks]
    name_map = {s["ts_code"]: s["name"] for s in stocks}

    print(f"[INFO] 共需获取 {len(ts_codes)} 只股票数据")
    print(f"[INFO] 股票池: {ts_codes}")

    # 3. 计算日期范围
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    print(f"[INFO] 日期范围: {start_date} ~ {end_date}")

    # 4. 获取日线数据
    df = fetch_daily_data(pro, ts_codes, start_date, end_date)

    if df is None or df.empty:
        print("[ERROR] 未获取到任何数据")
        return pd.DataFrame()

    # 5. 验证每只股票是否都获取到数据
    fetched_codes = set(df["ts_code"].unique())
    missing_codes = set(ts_codes) - fetched_codes

    if missing_codes:
        print(f"[WARN] 以下股票未获取到数据: {missing_codes}")
    else:
        print(f"[OK] 所有 {len(ts_codes)} 只股票数据获取成功")

    # 6. 添加股票名称
    df["name"] = df["ts_code"].map(name_map)

    # 7. 打印每只股票的记录数
    print("\n[INFO] 各股票记录数:")
    for ts_code in ts_codes:
        count = len(df[df["ts_code"] == ts_code])
        name = name_map.get(ts_code, "")
        status = "[OK]" if count > 0 else "[MISSING]"
        print(f"  {status} {ts_code} {name}: {count} 条记录")

    # 8. 确保输出目录存在
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    # 9. 保存日线数据
    df.to_parquet(output_path, index=False)
    print(f"\n[INFO] 数据已保存至 {output_path}")
    print(f"[INFO] 总记录数: {len(df)}")
    print(f"[INFO] 列名: {df.columns.tolist()}")

    # 10. 获取 anchor 的 daily_basic（PE/PB/市值）
    anchor_code = config.get("anchor", {}).get("code", "")
    if anchor_code:
        _fetch_daily_basic(pro, anchor_code, start_date, end_date, output_dir)
        _fetch_moneyflow(pro, anchor_code, start_date, end_date, output_dir)

    return df


def _fetch_daily_basic(pro, ts_code: str, start_date: str, end_date: str, output_dir: Path):
    """获取每日基本面指标（PE/PB/市值），保存到 raw 层"""
    try:
        df = pro.daily_basic(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,pe,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate,turnover_rate_f,free_share"
        )
        if df is not None and not df.empty:
            out = output_dir / "daily_basic.parquet"
            df.to_parquet(out, index=False)
            print(f"[INFO] daily_basic: {len(df)} 条 → {out}")
        else:
            print("[WARN] daily_basic: 返回空数据")
    except Exception as e:
        print(f"[WARN] daily_basic 获取失败（不影响主流程）: {e}")


def _fetch_moneyflow(pro, ts_code: str, start_date: str, end_date: str, output_dir: Path):
    """获取资金流向数据，保存到 raw 层"""
    try:
        df = pro.moneyflow(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="ts_code,trade_date,buy_sm_vol,sell_sm_vol,buy_md_vol,sell_md_vol,buy_lg_vol,sell_lg_vol,buy_elg_vol,sell_elg_vol,net_mf_vol,net_mf_amount"
        )
        if df is not None and not df.empty:
            out = output_dir / "moneyflow.parquet"
            df.to_parquet(out, index=False)
            print(f"[INFO] moneyflow: {len(df)} 条 → {out}")
        else:
            print("[WARN] moneyflow: 返回空数据")
    except Exception as e:
        print(f"[WARN] moneyflow 获取失败（不影响主流程）: {e}")


if __name__ == "__main__":
    df = fetch_market_data()
    print("\n数据概览:")
    print(df.head(10))
    print(f"\n股票数量: {df['ts_code'].nunique()}")