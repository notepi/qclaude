"""
当前 schema 下的高风险回归测试。
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def _write_config(path: Path, anchor_code: str = "688333.SH", core_codes=None) -> Path:
    core_codes = core_codes or ["600118.SH"]
    core_entries = "\n".join(
        f"""  - code: {code}
    name: 股票{idx}
    active: true"""
        for idx, code in enumerate(core_codes, start=1)
    )
    content = f"""
version: "test"
anchor:
  code: {anchor_code}
  name: 锚定标的
core_universe:
{core_entries}
"""
    path.write_text(content.strip() + "\n", encoding="utf-8")
    return path


class TestCurrentSchema:
    def test_run_from_any_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        from analyzer import PROJECT_ROOT, DEFAULT_CONFIG_PATH, DEFAULT_DATA_PATH

        assert PROJECT_ROOT.is_absolute()
        assert DEFAULT_CONFIG_PATH == PROJECT_ROOT / "config" / "stocks.yaml"
        assert DEFAULT_DATA_PATH == PROJECT_ROOT / "data" / "normalized" / "market_data_normalized.parquet"

    def test_load_config_invalid_yaml(self, tmp_path):
        from analyzer import load_config

        bad_config = tmp_path / "bad_config.yaml"
        bad_config.write_text("invalid: yaml: content: [", encoding="utf-8")

        with pytest.raises(ValueError) as exc_info:
            load_config(str(bad_config))

        assert "配置文件格式错误" in str(exc_info.value)

    def test_load_config_missing_fields(self, tmp_path):
        from analyzer import load_config

        config = tmp_path / "incomplete.yaml"
        config.write_text("anchor: {}\n", encoding="utf-8")

        with pytest.raises(ValueError) as exc_info:
            load_config(str(config))

        assert "缺少必要字段" in str(exc_info.value)

    def test_empty_anchor_data(self, tmp_path, monkeypatch):
        import analyzer

        config_file = _write_config(tmp_path / "config.yaml", anchor_code="999999.SH")
        market_df = pd.DataFrame(
            {
                "ts_code": ["000001.SH", "000001.SH"],
                "trade_date": ["20240101", "20240102"],
                "close": [10.0, 10.5],
                "amount": [1000.0, 1100.0],
            }
        )
        monkeypatch.setattr(analyzer, "load_market_data", lambda _: market_df.copy())

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_anchor_symbol(str(config_file), "unused.parquet")

        assert "锚定标的" in str(exc_info.value) and "数据为空" in str(exc_info.value)

    def test_sector_coverage_error(self, tmp_path, monkeypatch):
        import analyzer

        core_codes = [f"68830{i}.SH" for i in range(1, 10)]
        config_file = _write_config(tmp_path / "config.yaml", anchor_code="688300.SH", core_codes=core_codes)
        market_df = pd.DataFrame(
            {
                "ts_code": ["688300.SH", "688300.SH", "688301.SH", "688301.SH"],
                "trade_date": ["20240101", "20240102", "20240101", "20240102"],
                "close": [10.0, 10.5, 20.0, 20.5],
                "amount": [1000.0, 1100.0, 2000.0, 2100.0],
            }
        )
        monkeypatch.setattr(analyzer, "load_market_data", lambda _: market_df.copy())

        with pytest.raises(ValueError) as exc_info:
            analyzer.analyze_anchor_symbol(str(config_file), "unused.parquet")

        assert "板块覆盖度不足" in str(exc_info.value)

    def test_price_data_product_missing_market_file_is_error(self, monkeypatch):
        import price_data_product

        monkeypatch.setattr(price_data_product, "MARKET_NORMALIZED_PATH", Path("/nonexistent/market_data_normalized.parquet"))
        monkeypatch.setattr(price_data_product, "MARKET_RAW_PATH", Path("/nonexistent/market_data.parquet"))

        result = price_data_product._load_market_data_with_status()
        assert result["status"] == "error"

    def test_report_ignores_stale_events_file(self, tmp_path, monkeypatch):
        import reporter

        metrics_path = tmp_path / "daily_metrics.parquet"
        pd.DataFrame(
            [
                {
                    "trade_date": pd.Timestamp("2024-01-02"),
                    "anchor_symbol": "688333.SH",
                    "anchor_return": 0.01,
                    "sector_avg_return": 0.005,
                    "core_universe_count": 1,
                    "sector_total_count": 1,
                    "relative_strength": 0.005,
                    "anchor_amount": 1000.0,
                    "amount_20d_high": False,
                    "amount_vs_5d_avg": 1.0,
                    "return_rank_in_sector": 1,
                    "amount_rank_in_sector": 1,
                    "sector_total_size": 2,
                    "price_strength_label": "中",
                    "volume_strength_label": "中",
                    "overall_signal_label": "中性",
                    "abnormal_signals": [],
                    "capital_flow_label": "主力中性",
                    "activity_label": "正常",
                    "capital_structure_label": "方向不明",
                    "price_capital_relation_label": "中性",
                    "price_data_product_status": "ok",
                    "market_data_status": "ok",
                    "daily_basic_status": "empty",
                    "moneyflow_status": "empty",
                }
            ]
        ).to_parquet(metrics_path)

        stale_events_path = tmp_path / "daily_events.json"
        stale_events_path.write_text(
            '{"trade_date":"20240101","event_signal_label":"有明确催化","event_summary":"stale"}',
            encoding="utf-8",
        )

        monkeypatch.setattr(reporter, "REPORTS_DIR", tmp_path / "reports")
        monkeypatch.setattr(reporter, "load_config", lambda _: {"anchor": {"code": "688333.SH", "name": "铂力特"}, "core_universe": [{"code": "600118.SH"}]})
        monkeypatch.setattr(reporter, "load_latest_metrics", lambda _: pd.read_parquet(metrics_path).iloc[0].to_dict())

        class _FakeRolling:
            @staticmethod
            def load_rolling_metrics():
                return None

        class _FakePriceProduct:
            @staticmethod
            def load_price_data_product():
                return {
                    "latest_trade_date": "20240102",
                    "overall_status": "ok",
                    "market_data_status": "ok",
                    "daily_basic_status": "empty",
                    "moneyflow_status": "empty",
                }

        class _FakeEventLayer:
            @staticmethod
            def load_events(_):
                import json
                with open(stale_events_path, "r", encoding="utf-8") as f:
                    return json.load(f)

        monkeypatch.setitem(sys.modules, "rolling_analyzer", _FakeRolling)
        monkeypatch.setitem(sys.modules, "price_data_product", _FakePriceProduct)
        monkeypatch.setitem(sys.modules, "event_layer", _FakeEventLayer)

        report_path = reporter.generate_daily_report(metrics_path=str(metrics_path), events_path=str(stale_events_path))
        content = Path(report_path).read_text(encoding="utf-8")
        assert "公司层" in content
        assert "板块层" in content
        assert "今日更多由板块联动与资金面主导" in content
