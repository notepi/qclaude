"""
历史回填模块 v1.0
逐日生成 archive/metrics/YYYYMMDD.parquet，完成后重算 rolling_metrics

用法：
  python backfill.py --days 10
  python backfill.py --start-date 20260101 --end-date 20260317
  python backfill.py --days 5 --dry-run
  python backfill.py --days 30 --force

参数说明：
  --days N          回填最近 N 个自然日（与 --start-date/--end-date 互斥）
  --start-date      起始日期，格式 YYYYMMDD 或 YYYY-MM-DD
  --end-date        结束日期，格式 YYYYMMDD 或 YYYY-MM-DD（默认：今天）
  --dry-run         只打印计划，不写文件
  --force           强制覆盖已存在的 archive 文件（默认跳过）

设计原则：
  - 优先复用 analyzer.analyze_anchor_symbol(as_of_date=...)
  - 默认跳过 events（不调用 event_layer）
  - 不改主 pipeline
  - 完成后自动重算 rolling_metrics
"""

import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

# 确保 src/ 在 path 中（直接运行时）
SRC_DIR = Path(__file__).parent
PROJECT_ROOT = SRC_DIR.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

ARCHIVE_METRICS_DIR = PROJECT_ROOT / "archive" / "metrics"


# ─────────────────────────────────────────────
# 日期工具
# ─────────────────────────────────────────────

def _parse_date(s: str) -> datetime:
    """解析 YYYYMMDD 或 YYYY-MM-DD 为 datetime"""
    s = s.strip().replace("-", "")
    return datetime.strptime(s, "%Y%m%d")


def _date_range(start: datetime, end: datetime):
    """生成 [start, end] 的逐日列表（含两端）"""
    dates = []
    cur = start
    while cur <= end:
        dates.append(cur)
        cur += timedelta(days=1)
    return dates


def _available_trade_dates(as_of: datetime) -> set:
    """
    从 normalized/raw 数据中获取所有可用交易日（<= as_of），
    用于判断某天是否是真实交易日。
    """
    try:
        from normalizer import load_normalized
        df = load_normalized()
    except Exception:
        try:
            raw_path = PROJECT_ROOT / "data" / "raw" / "market_data.parquet"
            df = pd.read_parquet(raw_path)
        except Exception:
            return set()

    if df.empty or "trade_date" not in df.columns:
        return set()

    if df["trade_date"].dtype.kind != "M":
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")

    dates = df["trade_date"].dropna().unique()
    return {pd.Timestamp(d).date() for d in dates if pd.Timestamp(d) <= pd.Timestamp(as_of)}


# ─────────────────────────────────────────────
# 单日回填
# ─────────────────────────────────────────────

def backfill_one_day(
    date: datetime,
    dry_run: bool = False,
    force: bool = False,
) -> bool:
    """
    回填单日指标到 archive/metrics/YYYYMMDD.parquet

    Returns:
        True  = 成功写入（或 dry_run 模拟成功）
        False = 跳过（已存在且非 force）或失败
    """
    date_str = date.strftime("%Y%m%d")
    out_path = ARCHIVE_METRICS_DIR / f"{date_str}.parquet"

    # 已存在且不强制覆盖 → 跳过
    if out_path.exists() and not force:
        print(f"  [SKIP] {date_str} 已存在，跳过（--force 可覆盖）")
        return False

    if dry_run:
        print(f"  [DRY]  {date_str} → {out_path}")
        return True

    try:
        from analyzer import analyze_anchor_symbol
        result = analyze_anchor_symbol(as_of_date=date)
    except ValueError as e:
        print(f"  [WARN] {date_str} 计算失败（跳过）: {e}")
        return False
    except Exception as e:
        print(f"  [ERROR] {date_str} 意外错误: {e}")
        return False

    # 验证计算出的日期与目标日期一致
    computed_date = pd.Timestamp(result["trade_date"]).date()
    target_date = date.date()
    if computed_date != target_date:
        print(
            f"  [WARN] {date_str} 日期不匹配：计算结果为 {computed_date}，"
            f"期望 {target_date}（该日可能非交易日，跳过）"
        )
        return False

    # 写入 archive
    ARCHIVE_METRICS_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([result])
    df.to_parquet(out_path, index=False)
    print(f"  [OK]   {date_str} → {out_path.name}")
    return True


# ─────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────

def run_backfill(
    dates: list,
    dry_run: bool = False,
    force: bool = False,
) -> dict:
    """
    批量回填指定日期列表

    Returns:
        {"written": int, "skipped": int, "failed": int}
    """
    total = len(dates)
    written = skipped = failed = 0

    print(f"\n{'='*55}")
    print(f"backfill: 共 {total} 个日期，dry_run={dry_run}, force={force}")
    print(f"{'='*55}")

    for i, date in enumerate(dates, 1):
        print(f"[{i:>3}/{total}] {date.strftime('%Y-%m-%d')}")
        result = backfill_one_day(date, dry_run=dry_run, force=force)
        if result is True:
            written += 1
        elif result is False:
            # 区分 skip 和 fail：backfill_one_day 内部已打印原因
            # 简单统计：已存在=skip，其余=fail（通过日志区分）
            skipped += 1

    print(f"\n{'='*55}")
    print(f"回填完成: 写入={written}, 跳过={skipped}")
    print(f"{'='*55}\n")

    return {"written": written, "skipped": skipped}


def recompute_rolling(dry_run: bool = False):
    """回填完成后重算 rolling_metrics"""
    if dry_run:
        print("[DRY] 跳过 rolling_metrics 重算")
        return

    print("重算 rolling_metrics...")
    try:
        from rolling_analyzer import compute_rolling_metrics
        result = compute_rolling_metrics()
        if result:
            print(f"[OK] rolling_metrics 重算完成: "
                  f"价格={result.get('price_trend_label')} | "
                  f"量能={result.get('volume_trend_label')} | "
                  f"动量={result.get('momentum_label')}")
        else:
            print("[WARN] rolling_metrics 返回空结果")
    except Exception as e:
        print(f"[ERROR] rolling_metrics 重算失败: {e}")


# ─────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="历史指标回填工具 v1.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python backfill.py --days 10
  python backfill.py --start-date 20260101 --end-date 20260317
  python backfill.py --days 5 --dry-run
  python backfill.py --days 30 --force
        """,
    )

    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        "--days", type=int, metavar="N",
        help="回填最近 N 个自然日（从今天往前数）",
    )
    date_group.add_argument(
        "--start-date", metavar="YYYYMMDD",
        help="起始日期（与 --end-date 配合使用）",
    )

    parser.add_argument(
        "--end-date", metavar="YYYYMMDD",
        help="结束日期（默认：今天）",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="只打印计划，不写文件",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="强制覆盖已存在的 archive 文件",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # 解析日期范围
    if args.days is not None:
        end_dt = today
        start_dt = today - timedelta(days=args.days - 1)
    elif args.start_date is not None:
        start_dt = _parse_date(args.start_date)
        end_dt = _parse_date(args.end_date) if args.end_date else today
    else:
        # 默认：回填最近 7 天
        print("[INFO] 未指定日期范围，默认回填最近 7 天")
        end_dt = today
        start_dt = today - timedelta(days=6)

    if start_dt > end_dt:
        print(f"[ERROR] start-date ({start_dt.date()}) > end-date ({end_dt.date()})")
        sys.exit(1)

    dates = _date_range(start_dt, end_dt)

    # 过滤非交易日（用实际数据中存在的日期判断）
    print("检测可用交易日...")
    trade_dates_set = _available_trade_dates(end_dt)

    if trade_dates_set:
        trade_dates = [d for d in dates if d.date() in trade_dates_set]
        skipped_non_trade = len(dates) - len(trade_dates)
        if skipped_non_trade > 0:
            print(f"[INFO] 过滤掉 {skipped_non_trade} 个非交易日")
        dates = trade_dates
    else:
        print("[WARN] 无法获取交易日历，将尝试所有日期（非交易日会在计算时自动跳过）")

    if not dates:
        print("[INFO] 没有需要回填的日期，退出")
        sys.exit(0)

    print(f"[INFO] 回填范围: {dates[0].strftime('%Y-%m-%d')} ~ {dates[-1].strftime('%Y-%m-%d')}，共 {len(dates)} 个交易日")

    # 执行回填
    stats = run_backfill(dates, dry_run=args.dry_run, force=args.force)

    # 重算 rolling_metrics
    if stats["written"] > 0 or args.dry_run:
        recompute_rolling(dry_run=args.dry_run)
    else:
        print("[INFO] 无新数据写入，跳过 rolling_metrics 重算")


if __name__ == "__main__":
    main()
