import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))


def test_announcement_provider_filters_by_sec_code(monkeypatch):
    import event_layer

    class _FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "announcements": [
                    {
                        "secCode": "688333",
                        "announcementTitle": "<em>铂力特</em>关于订单公告",
                        "announcementTime": 1704153600000,
                        "adjunctUrl": "finalpage/2024-01-02/test1.PDF",
                    },
                    {
                        "secCode": "000001",
                        "announcementTitle": "<em>铂力特</em>同名干扰公告",
                        "announcementTime": 1704153600000,
                        "adjunctUrl": "finalpage/2024-01-02/test2.PDF",
                    },
                ]
            }

    import requests

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: _FakeResponse())

    provider = event_layer.AnnouncementProvider()
    items, status, error = provider.fetch("铂力特", "688333.SH", "20240102")

    assert status == "ok"
    assert error is None
    assert len(items) == 1
    assert items[0]["title"] == "铂力特关于订单公告"


def test_build_events_list_uses_unique_ids_by_event_type():
    import event_layer

    analysis_context = event_layer._build_news_analysis_context(
        {
            "anchor": {"code": "688333.SH", "name": "铂力特"},
            "core_universe": [{"name": "中国卫星", "active": True}],
            "research_core": [],
            "extended_universe": [],
            "research_candidates": [],
            "trading_candidates": [],
            "event_keywords": {"sector": ["商业航天", "3D打印"]},
        },
        "688333.SH",
        "铂力特",
    )

    events = event_layer.build_events_list(
        trade_date="20240102",
        anchor_code="688333.SH",
        anchor_name="铂力特",
        announcements=[],
        announcement_status="empty",
        company_news=[{"title": "同一标题", "datetime": "2024-01-02 10:00:00", "source": "A"}],
        company_news_status="ok",
        sector_news=[{"title": "同一标题", "datetime": "2024-01-02 11:00:00", "source": "B", "matched_keyword": "商业航天"}],
        sector_news_status="ok",
        fetched_at="2024-01-02 12:00:00",
        analysis_context=analysis_context,
    )

    assert len(events) == 2
    assert events[0]["event_id"] != events[1]["event_id"]
    assert events[1]["keyword_hits"] == ["商业航天"]
    assert events[0]["source_level"] == "L2"
    assert events[0]["relevance_bucket"] == "company_direct"
    assert events[1]["source_level"] == "L2"
    assert events[1]["relevance_bucket"] == "pool_core"


def test_signal_and_summary_do_not_treat_errors_as_empty():
    import event_layer

    relevance_counts = {
        "company_direct_count": 0,
        "pool_core_count": 0,
        "pool_extended_count": 0,
        "background_count": 0,
        "noise_count": 0,
    }
    signal = event_layer.compute_event_signal_label(
        announcements=[],
        announcement_status="ok",
        company_news=[],
        company_news_status="error",
        sector_news=[],
        sector_news_status="timeout",
        relevance_counts=relevance_counts,
    )
    summary = event_layer.build_event_summary(
        trade_date="20240102",
        anchor_name="铂力特",
        announcements=[],
        announcement_status="ok",
        company_news=[],
        company_news_status="error",
        sector_news=[],
        sector_news_status="timeout",
        signal_label=signal,
        relevance_counts=relevance_counts,
    )

    assert signal == "信息不足"
    assert "公司层" in summary
    assert "公司新闻未获取（error）" in summary
    assert "核心板块层板块新闻未获取（timeout）" in summary


def test_3d_printing_background_news_does_not_raise_sector_signal():
    import event_layer

    analysis_context = event_layer._build_news_analysis_context(
        {
            "anchor": {"code": "688333.SH", "name": "铂力特"},
            "core_universe": [{"name": "中国卫星", "active": True}],
            "research_core": [],
            "extended_universe": [],
            "research_candidates": [],
            "trading_candidates": [],
            "event_keywords": {"sector": ["商业航天", "3D打印"]},
        },
        "688333.SH",
        "铂力特",
    )

    events = event_layer.build_events_list(
        trade_date="20240102",
        anchor_code="688333.SH",
        anchor_name="铂力特",
        announcements=[],
        announcement_status="empty",
        company_news=[],
        company_news_status="empty",
        sector_news=[
            {
                "title": "创想三维冲刺消费级3D打印第一股",
                "datetime": "2024-01-02 11:00:00",
                "source": "B",
                "matched_keyword": "3D打印",
            }
        ],
        sector_news_status="ok",
        fetched_at="2024-01-02 12:00:00",
        analysis_context=analysis_context,
    )

    counts = event_layer._summarize_relevance_counts(events)
    signal = event_layer.compute_event_signal_label(
        announcements=[],
        announcement_status="empty",
        company_news=[],
        company_news_status="empty",
        sector_news=[{}],
        sector_news_status="ok",
        relevance_counts=counts,
    )

    assert events[0]["relevance_bucket"] in {"pool_extended", "noise"}
    assert signal == "无明确催化"


def test_collect_events_builds_product_and_archive(monkeypatch, tmp_path):
    import event_layer

    output_path = tmp_path / "daily_events.json"
    archive_dir = tmp_path / "archive" / "events"

    monkeypatch.setattr(event_layer, "ARCHIVE_EVENTS_DIR", archive_dir)
    monkeypatch.setattr(
        event_layer,
        "load_config",
        lambda _=None: {
            "core_universe": [{"code": "600118.SH"}],
            "event_keywords": {"sector": ["商业航天"]},
        },
    )

    class _FakeAnnouncementProvider:
        def fetch(self, anchor_name, anchor_code, trade_date):
            return ([{"title": "订单公告", "date": "2024-01-02", "url": "http://example.com/a.pdf"}], "ok", None)

    class _FakeCompanyNewsProvider:
        def fetch(self, anchor_code, trade_date, max_items=3):
            return ([], "empty", None)

    class _FakeSectorNewsProvider:
        def fetch(self, sector_keywords, core_codes, trade_date, max_items=3):
            return ([{"title": "商业航天催化", "datetime": "2024-01-02 09:00:00", "source": "EM", "matched_keyword": ["商业航天"]}], "ok", None)

    monkeypatch.setattr(event_layer, "AnnouncementProvider", _FakeAnnouncementProvider)
    monkeypatch.setattr(event_layer, "CompanyNewsProvider", _FakeCompanyNewsProvider)
    monkeypatch.setattr(event_layer, "SectorNewsProvider", _FakeSectorNewsProvider)

    result = event_layer.collect_events(
        trade_date="20240102",
        anchor_code="688333.SH",
        anchor_name="铂力特",
        output_path=str(output_path),
    )

    assert result["latest_trade_date"] == "20240102"
    assert result["overall_status"] == "ok"
    assert result["provider_statuses"]["announcement"]["status"] == "ok"
    assert result["provider_statuses"]["company_news"]["status"] == "empty"
    assert result["company_direct_count"] == 1
    assert result["pool_core_count"] == 1
    assert len(result["events_list"]) == 2
    assert output_path.exists()
    assert (archive_dir / "20240102.json").exists()

    loaded = event_layer.load_news_data_product(str(output_path))
    assert loaded["event_signal_label"] == "有明确催化"
    assert loaded["events_list"][1]["keyword_hits"] == ["商业航天"]
    assert loaded["events_list"][1]["relevance_bucket"] == "pool_core"
