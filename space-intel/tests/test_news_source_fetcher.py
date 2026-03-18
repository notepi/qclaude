import json
import sys
from pathlib import Path
import tempfile

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def test_browser_session_source_returns_auth_required_without_local_session(monkeypatch):
    import news_source_fetcher

    monkeypatch.setattr(news_source_fetcher, "_find_browser_session", lambda: None)

    result = news_source_fetcher.fetch_registered_source(
        {
            "url": "https://tushare.pro/news/cls",
            "source_name": "tushare_cls",
            "source_level": "L2",
            "source_kind": "tushare_news_portal",
            "adapter_name": "tushare_cls",
            "fetch_mode": "browser_session",
        }
    )

    assert result["status"] == "auth_required"
    assert result["items"] == []
    assert "浏览器登录态目录" in result["error"]


def test_browser_session_source_returns_render_failed_without_playwright(monkeypatch):
    import news_source_fetcher

    monkeypatch.setattr(news_source_fetcher, "_find_browser_session", lambda: {"browser_name": "chrome", "profile_path": "/tmp/profile"})
    monkeypatch.setattr(news_source_fetcher, "_has_playwright", lambda: False)

    result = news_source_fetcher.fetch_registered_source(
        {
            "url": "https://tushare.pro/news/eastmoney",
            "source_name": "tushare_eastmoney",
            "source_level": "L2",
            "source_kind": "tushare_news_portal",
            "adapter_name": "tushare_eastmoney",
            "fetch_mode": "browser_session",
        }
    )

    assert result["status"] == "render_failed"
    assert "playwright" in result["error"]


def test_browser_session_source_returns_auth_required_when_page_redirects_to_login(monkeypatch):
    import news_source_fetcher

    monkeypatch.setattr(
        news_source_fetcher,
        "_find_browser_session",
        lambda: {
            "browser_name": "msedge",
            "profile_root": "/tmp/root",
            "profile_name": "Default",
            "profile_path": "/tmp/root/Default",
        },
    )
    monkeypatch.setattr(news_source_fetcher, "_has_playwright", lambda: True)
    monkeypatch.setattr(news_source_fetcher, "_copy_browser_profile", lambda session: Path("/tmp/copied-profile"))

    class _FakePage:
        url = "https://tushare.pro/weborder/#/login"

        def goto(self, *args, **kwargs):
            return None

        def wait_for_timeout(self, *args, **kwargs):
            return None

        def title(self):
            return "Tushare数据"

        def content(self):
            return "<html>wxLogin</html>"

    class _FakeContext:
        def new_page(self):
            return _FakePage()

        def close(self):
            return None

    class _FakeChromium:
        def launch_persistent_context(self, **kwargs):
            return _FakeContext()

    class _PlaywrightManager:
        def __enter__(self):
            class _FakePlaywright:
                chromium = _FakeChromium()

            return _FakePlaywright()

        def __exit__(self, exc_type, exc, tb):
            return None

    monkeypatch.setitem(sys.modules, "playwright.sync_api", type("M", (), {"sync_playwright": lambda: _PlaywrightManager()}))

    result = news_source_fetcher.fetch_registered_source(
        {
            "url": "https://tushare.pro/news/cls",
            "source_name": "tushare_cls",
            "source_level": "L2",
            "source_kind": "tushare_news_portal",
            "adapter_name": "tushare_cls",
            "fetch_mode": "browser_session",
        }
    )

    assert result["status"] == "auth_required"
    assert result["browser_name"] == "msedge"
    assert "登录态" in result["error"]


def test_fetch_configured_news_sources_writes_raw_products(tmp_path, monkeypatch):
    import news_source_fetcher

    registry = {
        "generated_at": "2026-03-18 10:00:00",
        "source_count": 1,
        "sources": [
            {
                "url": "https://tushare.pro/news/cls",
                "source_name": "tushare_cls",
                "source_level": "L2",
                "source_kind": "tushare_news_portal",
                "adapter_name": "tushare_cls",
                "fetch_mode": "browser_session",
                "enabled": True,
            }
        ],
    }

    class _FakeNewsSources:
        @staticmethod
        def load_news_sources_registry(_=None):
            return registry

        @staticmethod
        def sync_news_sources_registry(config_path=None):
            return registry

    monkeypatch.setitem(sys.modules, "news_sources", _FakeNewsSources)
    monkeypatch.setattr(
        news_source_fetcher,
        "fetch_registered_source",
        lambda source: {
            "source_url": source["url"],
            "source_name": source["source_name"],
            "source_level": source["source_level"],
            "source_kind": source["source_kind"],
            "adapter_name": source["adapter_name"],
            "fetch_mode": source["fetch_mode"],
            "status": "ok",
            "error": None,
            "fetched_at": "2026-03-18 10:01:00",
            "items": [
                {
                    "title": "测试标题",
                    "url": "https://example.com/1",
                    "published_at": "2026-03-18 09:00:00",
                    "summary": "测试摘要",
                }
            ],
        },
    )

    result = news_source_fetcher.fetch_configured_news_sources(output_dir=str(tmp_path))
    latest_path = tmp_path / "latest.json"
    source_path = tmp_path / "tushare_cls.json"

    assert result["source_count"] == 1
    assert latest_path.exists()
    assert source_path.exists()

    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest["results"][0]["status"] == "ok"

    normalized = news_source_fetcher.normalize_fetched_source_items(result)
    assert normalized[0]["title"] == "测试标题"
    assert normalized[0]["source_name"] == "tushare_cls"


def test_parse_tushare_news_items_extracts_titles():
    import news_source_fetcher

    text = """首页
财联社
19:48
【楚环科技：股东龚双红拟减持不超0.86%股份】财联社3月18日电，楚环科技公告称...
19:45
【康力电梯：高级管理人员沈舟群拟减持不超0.0353%股份】财联社3月18日电...
"""

    items = news_source_fetcher._parse_tushare_news_items(
        text,
        {"url": "https://tushare.pro/news/cls"},
    )

    assert len(items) == 2
    assert "楚环科技" in items[0]["title"]
    assert items[0]["published_at"] == "19:48"


def test_fetch_registered_source_prefers_cdp_when_available(monkeypatch):
    import news_source_fetcher

    cdp_result = {
        "source_url": "https://tushare.pro/news/cls",
        "source_name": "tushare_cls",
        "source_level": "L2",
        "source_kind": "tushare_news_portal",
        "adapter_name": "tushare_cls",
        "fetch_mode": "browser_session",
        "browser_name": "msedge_cdp",
        "profile_path": "remote-debugging-session",
        "status": "ok",
        "error": None,
        "fetched_at": "2026-03-18 20:00:00",
        "items": [{"title": "测试", "url": "u", "published_at": "20:00", "summary": "测试"}],
    }

    monkeypatch.setattr(news_source_fetcher, "_fetch_via_cdp_session", lambda source: cdp_result)

    result = news_source_fetcher.fetch_registered_source(
        {
            "url": "https://tushare.pro/news/cls",
            "source_name": "tushare_cls",
            "source_level": "L2",
            "source_kind": "tushare_news_portal",
            "adapter_name": "tushare_cls",
            "fetch_mode": "browser_session",
        }
    )

    assert result["status"] == "ok"
    assert result["browser_name"] == "msedge_cdp"


def test_find_browser_session_prefers_dedicated_edge_profile(monkeypatch):
    import news_source_fetcher

    with tempfile.TemporaryDirectory() as tmpdir:
        dedicated_root = Path(tmpdir) / ".edge-codex-debug"
        dedicated_profile = dedicated_root / "Default"
        dedicated_profile.mkdir(parents=True)

        monkeypatch.setattr(news_source_fetcher, "DEDICATED_EDGE_PROFILE_ROOT", dedicated_root)
        monkeypatch.setattr(Path, "home", lambda: Path(tmpdir))

        session = news_source_fetcher._find_browser_session()

        assert session is not None
        assert session["browser_name"] == "msedge"
        assert session["profile_root"] == str(dedicated_root)
        assert session["profile_path"] == str(dedicated_profile)


def test_dedicated_edge_launch_command_points_to_persistent_profile():
    import news_source_fetcher

    command = news_source_fetcher._dedicated_edge_launch_command()

    assert "Microsoft Edge" in command
    assert "--remote-debugging-port=9222" in command
    assert str(news_source_fetcher.DEDICATED_EDGE_PROFILE_ROOT) in command


def test_choose_cdp_page_prefers_exact_tushare_tab():
    import news_source_fetcher

    class _FakePage:
        def __init__(self, url):
            self.url = url

    class _FakeContext:
        pages = [
            _FakePage("edge://newtab/"),
            _FakePage("https://tushare.pro/news/eastmoney"),
            _FakePage("https://tushare.pro/news/cls"),
        ]

    page, should_navigate = news_source_fetcher._choose_cdp_page(
        _FakeContext(),
        {"url": "https://tushare.pro/news/cls"},
    )

    assert page.url == "https://tushare.pro/news/cls"
    assert should_navigate is False


def test_is_login_page_does_not_misclassify_normal_news_page():
    import news_source_fetcher

    class _FakePage:
        url = "https://tushare.pro/news/eastmoney"

        def title(self):
            return "Tushare数据"

        def content(self):
            return "<html><body>首页 资讯数据 登录 财联社 19:59 【测试新闻】内容</body></html>"

    assert news_source_fetcher._is_login_page(_FakePage()) is False
