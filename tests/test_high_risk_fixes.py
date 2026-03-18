"""
高风险问题修复测试
验证 H1-H4 四个问题的修复效果
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path

import pandas as pd
import pytest

# 添加 src 目录到路径
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


class TestH1PathHardcoding:
    """测试 H1: 路径硬编码问题"""

    def test_run_from_any_directory(self, tmp_path, monkeypatch):
        """验证可以从任意目录运行"""
        # 切换到临时目录
        monkeypatch.chdir(tmp_path)

        # 导入模块（应该不会因为工作目录改变而失败）
        from analyzer import PROJECT_ROOT, DEFAULT_CONFIG_PATH, DEFAULT_DATA_PATH

        # 验证路径是绝对路径且正确
        assert PROJECT_ROOT.is_absolute()
        assert DEFAULT_CONFIG_PATH.is_absolute()
        assert DEFAULT_DATA_PATH.is_absolute()

        # 验证路径指向正确的位置
        assert PROJECT_ROOT.name == "space-intel"
        assert DEFAULT_CONFIG_PATH == PROJECT_ROOT / "config" / "stocks.yaml"
        assert DEFAULT_DATA_PATH == PROJECT_ROOT / "data" / "raw" / "market_data.parquet"

    def test_fetcher_paths(self):
        """验证 fetcher 模块路径正确"""
        from fetcher import PROJECT_ROOT, DEFAULT_CONFIG_PATH, DEFAULT_OUTPUT_PATH

        assert PROJECT_ROOT.is_absolute()
        assert DEFAULT_CONFIG_PATH.is_absolute()
        assert DEFAULT_OUTPUT_PATH.is_absolute()

    def test_storage_paths(self):
        """验证 storage 模块路径正确"""
        from storage import PROJECT_ROOT, RAW_DIR, PROCESSED_DIR

        assert PROJECT_ROOT.is_absolute()
        assert RAW_DIR.is_absolute()
        assert PROCESSED_DIR.is_absolute()


class TestH2ExceptionHandling:
    """测试 H2: 数据加载异常处理"""

    def test_load_config_file_not_found(self):
        """测试配置文件不存在时的错误"""
        from analyzer import load_config

        with pytest.raises(FileNotFoundError) as exc_info:
            load_config("/nonexistent/path/config.yaml")

        assert "配置文件不存在" in str(exc_info.value)

    def test_load_market_data_file_not_found(self):
        """测试数据文件不存在时的错误"""
        from analyzer import load_market_data

        with pytest.raises(FileNotFoundError) as exc_info:
            load_market_data("/nonexistent/path/data.parquet")

        assert "市场数据文件不存在" in str(exc_info.value)

    def test_load_metrics_file_not_found(self):
        """测试指标数据文件不存在时的错误"""
        from reporter import load_latest_metrics

        with pytest.raises(FileNotFoundError) as exc_info:
            load_latest_metrics("/nonexistent/path/metrics.parquet")

        assert "指标数据文件不存在" in str(exc_info.value)

    def test_load_config_invalid_yaml(self, tmp_path):
        """测试配置文件格式错误"""
        from analyzer import load_config

        # 创建无效的 YAML 文件
        bad_config = tmp_path / "bad_config.yaml"
        bad_config.write_text("invalid: yaml: content: [")

        with pytest.raises(ValueError) as exc_info:
            load_config(str(bad_config))

        assert "配置文件格式错误" in str(exc_info.value)

    def test_load_config_missing_fields(self, tmp_path):
        """测试配置文件缺少必要字段"""
        from analyzer import load_config

        # 创建缺少必要字段的配置文件
        incomplete_config = tmp_path / "incomplete.yaml"
        incomplete_config.write_text("some_field: value")

        with pytest.raises(ValueError) as exc_info:
            load_config(str(incomplete_config))

        assert "缺少必要字段" in str(exc_info.value)

    def test_load_market_data_empty_file(self, tmp_path):
        """测试空数据文件"""
        from analyzer import load_market_data

        # 创建空的 parquet 文件
        empty_file = tmp_path / "empty.parquet"
        pd.DataFrame().to_parquet(empty_file)

        with pytest.raises(ValueError) as exc_info:
            load_market_data(str(empty_file))

        assert "市场数据文件为空" in str(exc_info.value)

    def test_load_market_data_missing_columns(self, tmp_path):
        """测试数据文件缺少必要列"""
        from analyzer import load_market_data

        # 创建缺少必要列的 parquet 文件
        bad_data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        bad_file = tmp_path / "bad_data.parquet"
        bad_data.to_parquet(bad_file)

        with pytest.raises(ValueError) as exc_info:
            load_market_data(str(bad_file))

        assert "市场数据缺少必要列" in str(exc_info.value)


class TestH3EmptyDataProtection:
    """测试 H3: 空数据防护"""

    def test_empty_anchor_symbol_data(self, tmp_path):
        """测试锚定标的数据为空"""
        from analyzer import analyze_anchor_symbol

        # 创建配置文件
        config_content = """
anchor_symbol: 999999.SH
commercial_space_universe:
  - code: 999999.SH
    name: 测试股票
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        # 创建不包含锚定标的的数据文件
        df = pd.DataFrame({
            "ts_code": ["000001.SH", "000001.SH"],
            "trade_date": ["20240101", "20240102"],
            "close": [10.0, 10.5],
            "amount": [1000.0, 1100.0]
        })
        data_file = tmp_path / "data.parquet"
        df.to_parquet(data_file)

        with pytest.raises(ValueError) as exc_info:
            analyze_anchor_symbol(str(config_file), str(data_file))

        assert "锚定标的" in str(exc_info.value) and "数据为空" in str(exc_info.value)

    def test_insufficient_data_for_return(self, tmp_path):
        """测试数据不足无法计算涨跌幅"""
        from analyzer import analyze_anchor_symbol

        # 创建配置文件
        config_content = """
anchor_symbol: 688333.SH
commercial_space_universe:
  - code: 688333.SH
    name: 铂力特
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        # 只创建一条数据
        df = pd.DataFrame({
            "ts_code": ["688333.SH"],
            "trade_date": ["20240101"],
            "close": [10.0],
            "amount": [1000.0]
        })
        data_file = tmp_path / "data.parquet"
        df.to_parquet(data_file)

        with pytest.raises(ValueError) as exc_info:
            analyze_anchor_symbol(str(config_file), str(data_file))

        assert "数据不足" in str(exc_info.value) and "无法计算涨跌幅" in str(exc_info.value)


class TestH4SectorAlignment:
    """测试 H4: 板块交易日对齐"""

    def test_sector_coverage_warning(self, tmp_path, capsys):
        """测试板块覆盖度不足时的警告"""
        from analyzer import analyze_anchor_symbol

        # 创建配置文件（3只股票）
        config_content = """
anchor_symbol: 688333.SH
commercial_space_universe:
  - code: 688333.SH
    name: 铂力特
  - code: 600118.SH
    name: 中国卫星
  - code: 600879.SH
    name: 航天电子
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        # 创建数据文件：只有锚定标的有最新日期数据
        df = pd.DataFrame({
            "ts_code": ["688333.SH", "688333.SH", "600118.SH", "600118.SH"],
            "trade_date": ["20240101", "20240102", "20240101", "20240102"],
            "close": [10.0, 10.5, 20.0, 20.5],
            "amount": [1000.0, 1100.0, 2000.0, 2100.0]
        })
        data_file = tmp_path / "data.parquet"
        df.to_parquet(data_file)

        # 由于覆盖度不足（只有2/3 = 66.7%，大于50%），应该正常执行
        # 但会在控制台打印信息
        result = analyze_anchor_symbol(str(config_file), str(data_file))
        assert result['sector_stock_count'] == 2
        assert result['sector_total_count'] == 3

    def test_sector_coverage_error(self, tmp_path):
        """测试板块覆盖度严重不足时报错"""
        from analyzer import analyze_anchor_symbol

        # 创建配置文件（10只股票）
        stocks = [{"code": f"68830{i}.SH", "name": f"股票{i}"} for i in range(10)]
        config_content = f"""
anchor_symbol: 688300.SH
commercial_space_universe:
"""
        for s in stocks:
            config_content += f"  - code: {s['code']}\n    name: {s['name']}\n"

        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        # 创建数据文件：只有锚定标的和一只其他股票有数据
        df = pd.DataFrame({
            "ts_code": ["688300.SH", "688300.SH", "688301.SH", "688301.SH"],
            "trade_date": ["20240101", "20240102", "20240101", "20240102"],
            "close": [10.0, 10.5, 20.0, 20.5],
            "amount": [1000.0, 1100.0, 2000.0, 2100.0]
        })
        data_file = tmp_path / "data.parquet"
        df.to_parquet(data_file)

        # 覆盖度只有 2/10 = 20%，应该报错
        with pytest.raises(ValueError) as exc_info:
            analyze_anchor_symbol(str(config_file), str(data_file))

        assert "板块覆盖度不足" in str(exc_info.value)
        assert "20.0%" in str(exc_info.value) or "20%" in str(exc_info.value)

    def test_sector_stock_count_in_result(self, tmp_path):
        """测试结果中包含板块参与计算股票数"""
        from analyzer import analyze_anchor_symbol

        # 创建配置文件
        config_content = """
anchor_symbol: 688333.SH
commercial_space_universe:
  - code: 688333.SH
    name: 铂力特
  - code: 600118.SH
    name: 中国卫星
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)

        # 创建数据文件
        df = pd.DataFrame({
            "ts_code": ["688333.SH", "688333.SH", "600118.SH", "600118.SH"],
            "trade_date": ["20240101", "20240102", "20240101", "20240102"],
            "close": [10.0, 10.5, 20.0, 20.5],
            "amount": [1000.0, 1100.0, 2000.0, 2100.0]
        })
        data_file = tmp_path / "data.parquet"
        df.to_parquet(data_file)

        result = analyze_anchor_symbol(str(config_file), str(data_file))

        # 验证结果中包含板块股票数
        assert 'sector_stock_count' in result
        assert 'sector_total_count' in result
        assert result['sector_stock_count'] == 2
        assert result['sector_total_count'] == 2


class TestIntegration:
    """集成测试：从任意目录运行 pipeline"""

    def test_pipeline_run_from_temp_dir(self, monkeypatch):
        """验证 pipeline 可以从任意目录运行"""
        import tempfile

        # 切换到临时目录
        with tempfile.TemporaryDirectory() as tmp_dir:
            monkeypatch.chdir(tmp_dir)

            # 尝试导入和执行基本操作
            from analyzer import PROJECT_ROOT, load_config

            # 验证可以正常加载配置
            config = load_config()
            assert 'anchor_symbol' in config
            assert 'commercial_space_universe' in config

            # 验证 PROJECT_ROOT 是正确的
            assert (PROJECT_ROOT / "config" / "stocks.yaml").exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])