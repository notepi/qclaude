"""
端到端测试模块
验证整个 MVP 链路在正确标的（铂力特 688333.SH）下能够稳定运行

测试范围：config → fetcher → analyzer → reporter

关键一致性校验：
A. anchor_symbol 必须为 688333.SH
B. anchor_symbol 对应名称必须为"铂力特"
C. 股票池中的代码与名称必须一致
D. 报告中的标的名称、代码、指标必须一致
E. daily_metrics 中 anchor_symbol 必须与 config 中一致
"""

import sys
import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# 预期的锚定标的配置
EXPECTED_ANCHOR_SYMBOL = "688333.SH"
EXPECTED_ANCHOR_NAME = "铂力特"


class E2ETestRunner:
    """端到端测试运行器"""

    def __init__(self):
        self.passed_tests: List[str] = []
        self.failed_tests: List[Tuple[str, str]] = []
        self.config = None
        self.metrics = None
        self.report_path = None

    def record_pass(self, test_name: str):
        """记录通过的测试"""
        self.passed_tests.append(test_name)
        print(f"  [PASS] {test_name}")

    def record_fail(self, test_name: str, reason: str):
        """记录失败的测试"""
        self.failed_tests.append((test_name, reason))
        print(f"  [FAIL] {test_name}: {reason}")

    def test_config_anchor_symbol(self) -> bool:
        """测试 A: anchor_symbol 必须为 688333.SH"""
        test_name = "A. anchor_symbol 必须为 688333.SH"

        config_path = PROJECT_ROOT / "config" / "stocks.yaml"
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

        actual_anchor = self.config.get('anchor_symbol')

        if actual_anchor == EXPECTED_ANCHOR_SYMBOL:
            self.record_pass(test_name)
            return True
        else:
            self.record_fail(test_name, f"期望 {EXPECTED_ANCHOR_SYMBOL}, 实际 {actual_anchor}")
            return False

    def test_config_anchor_name(self) -> bool:
        """测试 B: anchor_symbol 对应名称必须为'铂力特'"""
        test_name = "B. anchor_symbol 对应名称必须为'铂力特'"

        # 在股票池中查找 anchor_symbol
        anchor_symbol = self.config.get('anchor_symbol')
        universe = self.config.get('commercial_space_universe', [])

        anchor_stock = None
        for stock in universe:
            if stock['code'] == anchor_symbol:
                anchor_stock = stock
                break

        if anchor_stock is None:
            self.record_fail(test_name, f"在股票池中未找到 {anchor_symbol}")
            return False

        actual_name = anchor_stock.get('name')

        if actual_name == EXPECTED_ANCHOR_NAME:
            self.record_pass(test_name)
            return True
        else:
            self.record_fail(test_name, f"期望 '{EXPECTED_ANCHOR_NAME}', 实际 '{actual_name}'")
            return False

    def test_stock_code_name_consistency(self) -> bool:
        """测试 C: 股票池中的代码与名称必须一致"""
        test_name = "C. 股票池中的代码与名称必须一致"

        universe = self.config.get('commercial_space_universe', [])

        # 已知的代码-名称映射（正确的对应关系）
        known_mappings = {
            "688333.SH": "铂力特",
            "600118.SH": "中国卫星",
            "600879.SH": "航天电子",
            "600343.SH": "航天动力",
            "600151.SH": "航天工程",
            "002179.SZ": "中航光电",
            "603678.SH": "火炬电子",
            "000733.SZ": "振华科技",
            "603267.SH": "鸿远电子",
            "688062.SH": "航天宏图",
            "688568.SH": "中科星图",
        }

        all_consistent = True
        for stock in universe:
            code = stock.get('code')
            name = stock.get('name')
            expected_name = known_mappings.get(code)

            if expected_name and name != expected_name:
                self.record_fail(test_name, f"{code}: 期望 '{expected_name}', 实际 '{name}'")
                all_consistent = False

        if all_consistent:
            self.record_pass(test_name)
            return True
        return False

    def test_metrics_anchor_consistency(self) -> bool:
        """测试 E: daily_metrics 中 anchor_symbol 必须与 config 中一致"""
        test_name = "E. daily_metrics 中 anchor_symbol 必须与 config 中一致"

        metrics_path = PROJECT_ROOT / "data" / "processed" / "daily_metrics.parquet"

        if not metrics_path.exists():
            self.record_fail(test_name, f"指标文件不存在: {metrics_path}")
            return False

        self.metrics = pd.read_parquet(metrics_path)
        metrics_anchor = self.metrics.iloc[0]['anchor_symbol']
        config_anchor = self.config.get('anchor_symbol')

        if metrics_anchor == config_anchor == EXPECTED_ANCHOR_SYMBOL:
            self.record_pass(test_name)
            return True
        else:
            self.record_fail(test_name,
                f"metrics={metrics_anchor}, config={config_anchor}, 期望={EXPECTED_ANCHOR_SYMBOL}")
            return False

    def test_report_consistency(self) -> bool:
        """测试 D: 报告中的标的名称、代码、指标必须一致"""
        test_name = "D. 报告中的标的名称、代码、指标必须一致"

        reports_dir = PROJECT_ROOT / "reports"
        if not reports_dir.exists():
            self.record_fail(test_name, "reports 目录不存在")
            return False

        # 查找最新的报告
        report_files = list(reports_dir.glob("*_blt_review.md"))
        if not report_files:
            self.record_fail(test_name, "未找到报告文件")
            return False

        self.report_path = max(report_files, key=lambda p: p.stat().st_mtime)

        with open(self.report_path, 'r', encoding='utf-8') as f:
            report_content = f.read()

        # 检查报告中是否包含正确的名称
        checks = [
            (EXPECTED_ANCHOR_NAME in report_content, f"报告未包含标的名称 '{EXPECTED_ANCHOR_NAME}'"),
            ("涨跌幅" in report_content, "报告缺少涨跌幅指标"),
            ("成交额" in report_content, "报告缺少成交额指标"),
            ("相对强弱" in report_content, "报告缺少相对强弱指标"),
        ]

        all_checks_passed = True
        for check, msg in checks:
            if not check:
                self.record_fail(test_name, msg)
                all_checks_passed = False

        if all_checks_passed:
            self.record_pass(test_name)
            return True
        return False

    def run_all_tests(self) -> Dict:
        """运行所有测试并返回结果摘要"""
        print("=" * 60)
        print("端到端测试开始")
        print("=" * 60)

        # 按顺序执行测试
        print("\n[1/5] 测试配置文件中的 anchor_symbol...")
        self.test_config_anchor_symbol()

        print("\n[2/5] 测试 anchor_symbol 对应名称...")
        self.test_config_anchor_name()

        print("\n[3/5] 测试股票池代码-名称一致性...")
        self.test_stock_code_name_consistency()

        print("\n[4/5] 测试 metrics 与 config 的一致性...")
        self.test_metrics_anchor_consistency()

        print("\n[5/5] 测试报告内容一致性...")
        self.test_report_consistency()

        # 输出摘要
        print("\n" + "=" * 60)
        print("测试结果摘要")
        print("=" * 60)

        return {
            "passed": self.passed_tests,
            "failed": self.failed_tests,
            "total_passed": len(self.passed_tests),
            "total_failed": len(self.failed_tests),
            "anchor_symbol": self.config.get('anchor_symbol') if self.config else None,
            "anchor_name": EXPECTED_ANCHOR_NAME,
            "report_file": str(self.report_path.name) if self.report_path else None
        }


def run_full_pipeline():
    """
    执行完整的端到端流水线
    包括数据获取、分析、报告生成
    """
    print("=" * 60)
    print("执行完整端到端流水线")
    print("=" * 60)

    # 1. 数据获取
    print("\n[Step 1/4] 数据获取...")
    from src.fetcher import fetch_market_data
    df = fetch_market_data()

    if df.empty:
        print("[ERROR] 数据获取失败")
        return None

    # 2. 指标分析
    print("\n[Step 2/4] 指标分析...")
    from src.analyzer import analyze_anchor_symbol, save_metrics
    result = analyze_anchor_symbol()
    save_metrics(result)

    # 3. 报告生成
    print("\n[Step 3/4] 报告生成...")
    from src.reporter import generate_daily_report
    report_path = generate_daily_report()

    # 4. 一致性校验
    print("\n[Step 4/4] 一致性校验...")
    runner = E2ETestRunner()
    summary = runner.run_all_tests()

    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="端到端测试")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="跳过数据获取步骤，使用现有数据")
    args = parser.parse_args()

    if args.skip_fetch:
        # 只运行一致性校验
        print("使用现有数据运行一致性校验...")
        runner = E2ETestRunner()
        summary = runner.run_all_tests()
    else:
        # 运行完整流水线
        summary = run_full_pipeline()

    # 最终输出
    print("\n" + "=" * 60)
    print("最终确认")
    print("=" * 60)
    print(f"anchor_symbol: {summary.get('anchor_symbol')}")
    print(f"标的名称: {summary.get('anchor_name')}")
    print(f"报告文件: {summary.get('report_file')}")
    print(f"通过测试: {summary.get('total_passed')}/5")
    print(f"失败测试: {summary.get('total_failed')}/5")

    if summary.get('failed'):
        print("\n失败的测试项:")
        for name, reason in summary.get('failed', []):
            print(f"  - {name}: {reason}")