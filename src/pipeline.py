"""
统一执行入口
串联主流程：fetcher → normalizer → price_data_product → analyzer → rolling_analyzer → event_layer → reporter → validation

运行模式：
  full run (默认):     拉数据 → 算指标 → 事件层 → 生成报告 → 校验
  --skip-fetch:        跳过数据拉取，基于现有数据
  --skip-events:       跳过事件层（不影响主流程）
  --skip-fetch --skip-events: 只重跑 analyzer + reporter
"""

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

# 项目根目录（基于本文件位置计算）
PROJECT_ROOT = Path(__file__).parent.parent

# 将 src 目录添加到 Python 路径
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# 数据目录
DATA_RAW_DIR = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
ARCHIVE_DIR = PROJECT_ROOT / "archive"
REPORTS_DIR = PROJECT_ROOT / "reports"

# 默认文件路径
DEFAULT_RAW_DATA = DATA_RAW_DIR / "market_data.parquet"
DEFAULT_METRICS = DATA_PROCESSED_DIR / "daily_metrics.parquet"
DEFAULT_EVENTS = PROJECT_ROOT / "data" / "processed" / "daily_events.json"
DEFAULT_PRICE_PRODUCT = DATA_PROCESSED_DIR / "price_data_product.json"


def print_banner():
    """打印启动横幅"""
    print("\n" + "=" * 60)
    print("  Space Intel - 商业航天板块每日复盘")
    print("=" * 60)
    print(f"  执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")


def print_stage(stage: str, status: str = "running"):
    """打印当前阶段状态"""
    status_icons = {
        "running": "[...]",
        "success": "[OK]",
        "failed": "[FAIL]",
        "skipped": "[SKIP]"
    }
    icon = status_icons.get(status, "[?]")
    print(f"\n{icon} {stage}")
    print("-" * 50)


def validate_raw_data() -> Tuple[bool, str]:
    """
    验证原始数据是否存在且完整

    Returns:
        (is_valid, message)
    """
    if not DEFAULT_RAW_DATA.exists():
        return False, f"原始数据文件不存在: {DEFAULT_RAW_DATA}"

    import pandas as pd
    try:
        df = pd.read_parquet(DEFAULT_RAW_DATA)
        if df.empty:
            return False, "原始数据文件为空"

        # 检查必要列
        required_cols = ["ts_code", "trade_date", "close", "amount"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            return False, f"缺少必要列: {missing_cols}"

        return True, f"原始数据有效 ({len(df)} 条记录, {df['ts_code'].nunique()} 只股票)"

    except Exception as e:
        return False, f"读取原始数据失败: {e}"


def validate_metrics() -> Tuple[bool, str]:
    """
    验证指标数据是否存在且完整

    Returns:
        (is_valid, message)
    """
    if not DEFAULT_METRICS.exists():
        return False, f"指标数据文件不存在: {DEFAULT_METRICS}"

    import pandas as pd
    try:
        df = pd.read_parquet(DEFAULT_METRICS)
        if df.empty:
            return False, "指标数据文件为空"

        # 检查必要字段
        required_fields = ["trade_date", "anchor_symbol", "anchor_return"]
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            return False, f"缺少必要字段: {missing_fields}"

        latest = df.iloc[0]
        return True, f"指标数据有效 (交易日期: {latest['trade_date']})"

    except Exception as e:
        return False, f"读取指标数据失败: {e}"


def validate_price_data_product() -> Tuple[bool, str]:
    """
    验证价格数据产品是否存在且可用

    Returns:
        (is_valid, message)
    """
    if not DEFAULT_PRICE_PRODUCT.exists():
        return False, f"价格数据产品不存在: {DEFAULT_PRICE_PRODUCT}"

    import json
    try:
        with open(DEFAULT_PRICE_PRODUCT, "r", encoding="utf-8") as f:
            product = json.load(f)
    except Exception as e:
        return False, f"读取价格数据产品失败: {e}"

    overall_status = product.get("overall_status")
    latest_trade_date = product.get("latest_trade_date")
    if overall_status == "error":
        return False, f"价格底座不可用: {product.get('market_data_reason') or overall_status}"
    if not latest_trade_date:
        return False, "价格数据产品缺少 latest_trade_date"

    return True, f"价格数据产品有效 ({latest_trade_date}, 状态: {overall_status})"


def validate_report(report_path: str) -> Tuple[bool, str]:
    """
    验证报告文件是否存在

    Returns:
        (is_valid, message)
    """
    report_file = Path(report_path)
    if not report_file.exists():
        return False, f"报告文件不存在: {report_path}"

    try:
        content = report_file.read_text(encoding='utf-8')
        if len(content) < 100:
            return False, "报告内容过短，可能生成失败"

        return True, f"报告有效 (文件大小: {len(content)} 字符)"

    except Exception as e:
        return False, f"读取报告失败: {e}"


def run_fetcher() -> Tuple[bool, str]:
    """
    执行数据获取阶段

    Returns:
        (success, message)
    """
    print_stage("Stage 1: 数据获取 (Fetcher)", "running")

    try:
        from fetcher import fetch_market_data

        df = fetch_market_data()

        if df is None or df.empty:
            return False, "数据获取失败：未获取到任何数据"

        return True, f"数据获取成功：{len(df)} 条记录"

    except Exception as e:
        return False, f"数据获取异常: {e}"


def run_normalizer() -> Tuple[bool, str]:
    """
    执行数据标准化阶段（raw → normalized）

    Returns:
        (success, message)
    """
    print_stage("Stage 2: 数据标准化 (Normalizer)", "running")

    try:
        from normalizer import normalize

        is_valid, msg = validate_raw_data()
        if not is_valid:
            return False, f"raw 数据验证失败: {msg}"

        normalize()
        return True, "数据标准化完成"

    except Exception as e:
        return False, f"数据标准化异常: {e}"


def archive_daily(trade_date: str) -> None:
    """
    归档当日 metrics 和 events 快照

    归档路径：
      archive/metrics/YYYYMMDD.parquet
      archive/events/YYYYMMDD.json

    规则：
      - 同一天已有归档时跳过（不覆盖）
      - 归档失败不影响主流程，只打印警告
    """
    import shutil

    # 归档 metrics
    metrics_archive_dir = ARCHIVE_DIR / "metrics"
    metrics_archive_dir.mkdir(parents=True, exist_ok=True)
    metrics_dst = metrics_archive_dir / f"{trade_date}.parquet"

    if DEFAULT_METRICS.exists():
        if metrics_dst.exists():
            print(f"[INFO] 归档跳过（已存在）: {metrics_dst.name}")
        else:
            shutil.copy2(DEFAULT_METRICS, metrics_dst)
            print(f"[INFO] 归档 metrics: {metrics_dst.name}")
    else:
        print(f"[WARN] 归档跳过：daily_metrics.parquet 不存在")

    # 归档 events
    events_archive_dir = ARCHIVE_DIR / "events"
    events_archive_dir.mkdir(parents=True, exist_ok=True)
    events_dst = events_archive_dir / f"{trade_date}.json"

    if DEFAULT_EVENTS.exists():
        if events_dst.exists():
            print(f"[INFO] 归档跳过（已存在）: {events_dst.name}")
        else:
            shutil.copy2(DEFAULT_EVENTS, events_dst)
            print(f"[INFO] 归档 events: {events_dst.name}")
    else:
        print(f"[INFO] 归档跳过：daily_events.json 不存在（事件层未运行）")


def run_archive() -> Tuple[bool, str]:
    """
    执行按日归档阶段

    Returns:
        (success, message)
    """
    print_stage("Stage 9: 按日归档 (Archive)", "running")

    try:
        import pandas as pd

        if not DEFAULT_METRICS.exists():
            return False, "daily_metrics.parquet 不存在，无法归档"

        df = pd.read_parquet(DEFAULT_METRICS)
        if df.empty:
            return False, "daily_metrics 为空，无法归档"

        latest = df.sort_values("trade_date", ascending=False).iloc[0]
        trade_date = pd.Timestamp(latest["trade_date"]).strftime("%Y%m%d")

        archive_daily(trade_date)
        return True, f"归档完成：{trade_date}"

    except Exception as e:
        return False, f"归档异常: {e}"


def run_analyzer() -> Tuple[bool, str]:
    """
    执行指标分析阶段

    Returns:
        (success, message)
    """
    print_stage("Stage 4: 指标分析 (Analyzer)", "running")

    try:
        from analyzer import analyze_anchor_symbol, save_metrics

        # 先验证价格数据产品
        is_valid, msg = validate_price_data_product()
        if not is_valid:
            return False, f"价格数据产品验证失败: {msg}"

        # 执行分析
        result = analyze_anchor_symbol()

        if not result:
            return False, "指标分析失败：未返回结果"

        # 保存结果
        save_metrics(result)

        return True, f"指标分析完成：交易日期 {result['trade_date']}"

    except Exception as e:
        return False, f"指标分析异常: {e}"


def run_price_data_product() -> Tuple[bool, str]:
    """
    构建价格数据产品

    Returns:
        (success, message)
    """
    print_stage("Stage 3: 价格底座 (Price Data Product)", "running")

    try:
        from price_data_product import build_price_data_product

        product = build_price_data_product()
        if product.get("overall_status") == "error":
            return False, f"价格底座构建失败: {product.get('market_data_reason') or product['overall_status']}"

        return True, (
            f"价格底座完成：{product['latest_trade_date']} | "
            f"overall={product['overall_status']} | "
            f"market={product['market_data_status']} | "
            f"basic={product['daily_basic_status']} | "
            f"moneyflow={product['moneyflow_status']}"
        )

    except Exception as e:
        return False, f"价格底座异常: {e}"


def run_rolling_analyzer() -> Tuple[bool, str]:
    """
    执行连续观察层计算（archive → rolling_metrics）

    Returns:
        (success, message)
    """
    print_stage("Stage 5: 连续观察层 (Rolling Analyzer)", "running")

    try:
        from rolling_analyzer import compute_rolling_metrics

        result = compute_rolling_metrics()
        if result is None:
            return True, "archive 数据不足，连续观察层跳过（不影响主流程）"
        return True, f"连续观察层完成：{result['momentum_label']}"

    except Exception as e:
        return False, f"连续观察层异常: {e}"


def run_event_layer() -> Tuple[bool, str]:
    """
    执行事件层阶段

    Returns:
        (success, message)
        注意：失败时返回 (False, msg) 但 pipeline 不会因此中止
    """
    print_stage("Stage 5: 事件层 (Event Layer)", "running")

    try:
        import pandas as pd
        from event_layer import collect_events
        from analyzer import load_config

        # 读取最新交易日和标的信息
        metrics_file = DATA_PROCESSED_DIR / "daily_metrics.parquet"
        if not metrics_file.exists():
            return False, "daily_metrics.parquet 不存在，请先运行 analyzer"

        df = pd.read_parquet(metrics_file)
        latest = df.sort_values('trade_date', ascending=False).iloc[0]
        trade_date = pd.Timestamp(latest['trade_date']).strftime('%Y%m%d')
        anchor_code = latest['anchor_symbol']

        config = load_config()
        anchor_name = config['anchor']['name']

        result = collect_events(trade_date, anchor_code, anchor_name)

        overall_status = result.get("overall_status", "error")
        if overall_status == "error":
            return False, f"新闻数据产品构建失败: {result.get('error') or overall_status}"

        return True, (
            f"新闻数据产品完成：{len(result['company_announcements'])} 条公告，"
            f"{len(result['company_news'])} 条公司新闻，"
            f"{len(result['sector_news'])} 条板块新闻，"
            f"overall={overall_status}，"
            f"信号：{result['event_signal_label']}"
        )

    except Exception as e:
        return False, f"事件层异常: {e}"


def run_reporter(include_events: bool = True) -> Tuple[bool, str, Optional[str]]:
    """
    执行报告生成阶段

    Returns:
        (success, message, report_path)
    """
    print_stage("Stage 6: 报告生成 (Reporter)", "running")

    try:
        from reporter import generate_daily_report

        # 先验证指标数据
        is_valid, msg = validate_metrics()
        if not is_valid:
            return False, f"指标数据验证失败: {msg}", None

        # 生成报告
        report_path = generate_daily_report(include_events=include_events)

        return True, f"报告生成成功: {report_path}", report_path

    except Exception as e:
        return False, f"报告生成异常: {e}", None


def run_e2e_validation(report_path: Optional[str] = None) -> Tuple[bool, str]:
    """
    执行端到端验证

    Returns:
        (success, message)
    """
    print_stage("Stage 7: E2E 验证 (Validation)", "running")

    errors = []

    # 1. 验证原始数据
    is_valid, msg = validate_raw_data()
    if is_valid:
        print(f"  [OK] 原始数据: {msg}")
    else:
        print(f"  [FAIL] 原始数据: {msg}")
        errors.append(f"原始数据: {msg}")

    # 2. 验证指标数据
    is_valid, msg = validate_metrics()
    if is_valid:
        print(f"  [OK] 指标数据: {msg}")
    else:
        print(f"  [FAIL] 指标数据: {msg}")
        errors.append(f"指标数据: {msg}")

    # 3. 验证报告文件
    if report_path:
        is_valid, msg = validate_report(report_path)
        if is_valid:
            print(f"  [OK] 报告文件: {msg}")
        else:
            print(f"  [FAIL] 报告文件: {msg}")
            errors.append(f"报告文件: {msg}")
    else:
        # 尝试查找最新的报告文件
        reports = list(REPORTS_DIR.glob("*_blt_review.md"))
        if reports:
            latest_report = sorted(reports)[-1]
            is_valid, msg = validate_report(str(latest_report))
            if is_valid:
                print(f"  [OK] 报告文件: {msg}")
            else:
                print(f"  [FAIL] 报告文件: {msg}")
                errors.append(f"报告文件: {msg}")
        else:
            print("  [WARN] 未找到报告文件")
            errors.append("报告文件: 未找到")

    if errors:
        return False, "验证失败: " + "; ".join(errors)
    return True, "所有验证通过"


def run_pipeline(skip_fetch: bool = False, skip_events: bool = False) -> int:
    """
    执行完整 pipeline v2.4

    执行顺序：
      fetcher → normalizer → price_data_product → analyzer → rolling_analyzer → event_layer → reporter → validation → archive
    """
    print_banner()

    results = {
        "fetcher":          None,
        "normalizer":       None,
        "price_data_product": None,
        "analyzer":         None,
        "rolling_analyzer": None,
        "event_layer":      None,
        "reporter":         None,
        "validation":       None,
        "archive":          None,
    }

    report_path = None

    # Stage 1: 数据获取（可选跳过）
    if skip_fetch:
        print_stage("Stage 1: 数据获取 (Fetcher)", "skipped")
        print("  跳过数据获取，使用现有数据")
        results["fetcher"] = "skipped"
    else:
        success, msg = run_fetcher()
        results["fetcher"] = "success" if success else "failed"
        if not success:
            print(f"\n[ERROR] {msg}")
            return 1

    # Stage 2: 数据标准化
    success, msg = run_normalizer()
    results["normalizer"] = "success" if success else "degraded"
    if not success:
        print(f"\n[WARN] normalizer 失败，analyzer 将降级从 raw 层读取: {msg}")

    # Stage 3: 价格底座
    success, msg = run_price_data_product()
    results["price_data_product"] = "success" if success else "failed"
    if not success:
        print(f"\n[ERROR] {msg}")
        return 1

    # Stage 4: 指标分析
    success, msg = run_analyzer()
    results["analyzer"] = "success" if success else "failed"
    if not success:
        print(f"\n[ERROR] {msg}")
        return 1

    # Stage 5: 连续观察层（失败不中止）
    success, msg = run_rolling_analyzer()
    if success:
        results["rolling_analyzer"] = "success"
    else:
        print(f"\n[WARN] 连续观察层失败（不影响主流程）: {msg}")
        results["rolling_analyzer"] = "failed"

    # Stage 6: 事件层（可选跳过，失败不中止）
    if skip_events:
        print_stage("Stage 5: 事件层 (Event Layer)", "skipped")
        print("  跳过事件层")
        results["event_layer"] = "skipped"
    else:
        success, msg = run_event_layer()
        if success:
            results["event_layer"] = "success"
        else:
            print(f"\n[WARN] 事件层失败（不影响主流程）: {msg}")
            results["event_layer"] = "failed"

    # Stage 7: 报告生成
    success, msg, report_path = run_reporter(include_events=not skip_events)
    results["reporter"] = "success" if success else "failed"
    if not success:
        print(f"\n[ERROR] {msg}")
        return 1

    # Stage 8: E2E 验证
    success, msg = run_e2e_validation(report_path)
    results["validation"] = "success" if success else "failed"
    if not success:
        print(f"\n[WARN] {msg}")

    # Stage 9: 按日归档
    success, msg = run_archive()
    results["archive"] = "success" if success else "failed"
    if not success:
        print(f"\n[WARN] 归档失败（不影响主流程）: {msg}")

    # 打印执行摘要
    print("\n" + "=" * 60)
    print("  执行摘要")
    print("=" * 60)
    print(f"  {'阶段':<24} {'状态':<10}")
    print("-" * 60)
    for stage, status in results.items():
        status_display = {
            "success":  "成功",
            "failed":   "失败",
            "skipped":  "跳过",
            "degraded": "降级",
            None:       "未执行"
        }.get(status, status)
        print(f"  {stage:<24} {status_display:<10}")

    print("-" * 60)
    if report_path:
        print(f"\n  报告路径: {report_path}")
    print("=" * 60 + "\n")

    if results["analyzer"] == "success" and results["reporter"] == "success":
        print("[OK] Pipeline 执行成功")
        return 0
    else:
        print("[FAIL] Pipeline 执行失败")
        return 1


def main():
    """命令行入口"""
    parser = argparse.ArgumentParser(
        description="Space Intel - 商业航天板块每日复盘 Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
运行模式:
  full run (默认):
    完整流程：拉数据 → 算指标 → 事件层 → 生成报告 → 校验

  --skip-fetch:
    跳过数据拉取：基于现有数据重新分析、生成报告、校验

  --skip-events:
    跳过事件层：报告中不含"今日可能驱动因素"部分

  --skip-fetch --skip-events:
    只重跑 analyzer + reporter

示例:
  python pipeline.py                          # 完整流程
  python pipeline.py --skip-fetch             # 跳过数据获取
  python pipeline.py --skip-fetch --skip-events  # 只重跑分析和报告
        """
    )

    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="跳过数据获取阶段，使用现有数据"
    )

    parser.add_argument(
        "--skip-events",
        action="store_true",
        help="跳过事件层（不影响主流程，报告中不含驱动因素部分）"
    )

    args = parser.parse_args()

    exit_code = run_pipeline(skip_fetch=args.skip_fetch, skip_events=args.skip_events)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
