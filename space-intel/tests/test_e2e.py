"""
当前配置 schema 的轻量端到端一致性测试。
"""

import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def test_config_anchor_and_core_schema():
    config_path = PROJECT_ROOT / "config" / "stocks.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    assert config["anchor"]["code"] == "688333.SH"
    assert config["anchor"]["name"] == "铂力特"
    assert len(config["core_universe"]) >= 1
    assert any(stock["code"] == "600118.SH" for stock in config["core_universe"])


def test_metrics_anchor_matches_config():
    pyarrow = pytest.importorskip("pyarrow")
    assert pyarrow is not None

    config_path = PROJECT_ROOT / "config" / "stocks.yaml"
    metrics_path = PROJECT_ROOT / "data" / "processed" / "daily_metrics.parquet"

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    metrics = pd.read_parquet(metrics_path)
    metrics_anchor = metrics.iloc[0]["anchor_symbol"]

    assert metrics_anchor == config["anchor"]["code"] == "688333.SH"


def test_report_contains_anchor_name():
    reports_dir = PROJECT_ROOT / "reports"
    report_files = list(reports_dir.glob("*_blt_review.md"))

    assert report_files, "未找到报告文件"

    latest_report = max(report_files, key=lambda p: p.stat().st_mtime)
    content = latest_report.read_text(encoding="utf-8")

    assert "铂力特" in content
    assert "成交额" in content
    assert "相对强弱" in content
