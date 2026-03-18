import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def test_load_news_sources_config_keeps_valid_urls_and_dedupes(tmp_path):
    import news_sources

    config_path = tmp_path / "news_sources.yaml"
    config_path.write_text(
        """
sources:
  - https://www.cninfo.com.cn/
  - https://www.cninfo.com.cn
  - invalid-url
  - ""
  - https://example.com/feed.xml
""".strip()
        + "\n",
        encoding="utf-8",
    )

    urls = news_sources.load_news_sources_config(str(config_path))
    assert urls == ["https://www.cninfo.com.cn/", "https://example.com/feed.xml"]


def test_classify_news_source_maps_known_domains():
    import news_sources

    cninfo = news_sources.classify_news_source("https://www.cninfo.com.cn/new/fulltextSearch/full")
    rss = news_sources.classify_news_source("https://example.com/feed.xml")
    tushare_cls = news_sources.classify_news_source("https://tushare.pro/news/cls")
    unknown = news_sources.classify_news_source("https://unknown.example.org/news")

    assert cninfo["source_level"] == "L1"
    assert cninfo["fetch_mode"] == "html_list"
    assert rss["fetch_mode"] == "rss"
    assert rss["source_level"] == "L2"
    assert tushare_cls["source_level"] == "L2"
    assert tushare_cls["source_kind"] == "tushare_news_portal"
    assert tushare_cls["fetch_mode"] == "browser_session"
    assert tushare_cls["adapter_name"] == "tushare_cls"
    assert unknown["source_level"] == "unknown"
    assert unknown["fetch_mode"] == "unsupported"


def test_sync_news_sources_registry_writes_independent_product(tmp_path):
    import json
    import news_sources

    config_path = tmp_path / "news_sources.yaml"
    output_path = tmp_path / "news_sources_registry.json"
    config_text = """
sources:
  - https://www.cninfo.com.cn/new/fulltextSearch/full
  - https://example.com/feed.xml
"""
    config_path.write_text(config_text.strip() + "\n", encoding="utf-8")

    registry = news_sources.sync_news_sources_registry(str(config_path), str(output_path))

    assert registry["source_count"] == 2
    assert output_path.exists()
    loaded = json.loads(output_path.read_text(encoding="utf-8"))
    assert loaded["sources"][0]["source_level"] == "L1"
    assert loaded["sources"][1]["fetch_mode"] == "rss"
    assert config_path.read_text(encoding="utf-8") == config_text.strip() + "\n"
