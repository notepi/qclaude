"""
新闻数据链模块 v1.0

职责：
  - 采集公告 / 公司新闻 / 板块新闻
  - 统一为标准化 events_list
  - 生成最新新闻数据产品 daily_events.json
  - 同时写入 archive/events/YYYYMMDD.json
"""

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
ARCHIVE_EVENTS_DIR = PROJECT_ROOT / "archive" / "events"

DEFAULT_CONFIG_PATH = CONFIG_DIR / "stocks.yaml"
DEFAULT_EVENTS_OUTPUT = DATA_PROCESSED_DIR / "daily_events.json"

_REQUEST_TIMEOUT = 8
_REQUEST_INTERVAL = 1.0

_DEFAULT_STRONG_THEME_TERMS = [
    "商业航天", "商业发射", "卫星互联网", "低轨卫星",
    "运载火箭", "火箭发动机", "航天发动机",
]
_DEFAULT_WEAK_THEME_TERMS = [
    "增材制造", "金属增材", "3D打印", "3D打印金属",
]
_DEFAULT_EXCLUDE_THEME_TERMS = [
    "消费级", "家用", "桌面级", "港股IPO", "冲刺港股IPO", "IPO",
]


def load_config(config_path: str = None) -> Dict[str, Any]:
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _to_date_str(trade_date: str) -> str:
    return f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"


def _unique_keep_order(values: List[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _match_terms(title: str, terms: List[str]) -> List[str]:
    return [term for term in terms if term and term in title]


def _build_news_analysis_context(config: Dict[str, Any], anchor_code: str, anchor_name: str) -> Dict[str, Any]:
    def _collect_names(section_name: str) -> List[str]:
        return [
            str(item.get("name", "")).strip()
            for item in config.get(section_name, [])
            if item.get("active", True) and str(item.get("name", "")).strip()
        ]

    event_cfg = config.get("event_keywords", {})
    configured_sector_terms = [str(term).strip() for term in event_cfg.get("sector", []) if str(term).strip()]
    configured_strong_terms = [
        term for term in configured_sector_terms
        if term in _DEFAULT_STRONG_THEME_TERMS or term not in _DEFAULT_WEAK_THEME_TERMS
    ]
    configured_weak_terms = [
        term for term in configured_sector_terms
        if term in _DEFAULT_WEAK_THEME_TERMS
    ]

    strong_themes = _unique_keep_order(_DEFAULT_STRONG_THEME_TERMS + configured_strong_terms)
    weak_themes = _unique_keep_order(_DEFAULT_WEAK_THEME_TERMS + configured_weak_terms)
    exclude_themes = _unique_keep_order(_DEFAULT_EXCLUDE_THEME_TERMS)

    core_names = _collect_names("core_universe") + _collect_names("research_core")
    extended_names = (
        _collect_names("extended_universe")
        + _collect_names("research_candidates")
        + _collect_names("trading_candidates")
    )

    return {
        "anchor_code": anchor_code,
        "anchor_name": anchor_name,
        "anchor_aliases": _unique_keep_order([anchor_name, anchor_code, anchor_code.split(".")[0]]),
        "core_names": _unique_keep_order(core_names),
        "extended_names": _unique_keep_order(extended_names),
        "strong_themes": strong_themes,
        "weak_themes": weak_themes,
        "exclude_themes": exclude_themes,
    }


def _classify_event(
    event_type: str,
    title: str,
    analysis_context: Dict[str, Any],
    keyword_hits: List[str],
) -> Dict[str, Any]:
    source_level = "L1" if event_type == "announcement" else "L2"
    title = title or ""

    anchor_hits = _match_terms(title, analysis_context["anchor_aliases"])
    core_hits = _match_terms(title, analysis_context["core_names"])
    extended_hits = _match_terms(title, analysis_context["extended_names"])
    strong_theme_hits = _unique_keep_order(
        _match_terms(title, analysis_context["strong_themes"])
        + [term for term in keyword_hits if term in analysis_context["strong_themes"]]
    )
    weak_theme_hits = _unique_keep_order(
        _match_terms(title, analysis_context["weak_themes"])
        + [term for term in keyword_hits if term in analysis_context["weak_themes"]]
    )
    exclude_hits = _match_terms(title, analysis_context["exclude_themes"])
    theme_hits = _unique_keep_order(keyword_hits + strong_theme_hits + weak_theme_hits + exclude_hits)

    if event_type in {"announcement", "company_news"}:
        return {
            "source_level": source_level,
            "relevance_level": "strong",
            "relevance_bucket": "company_direct",
            "pool_hits": [analysis_context["anchor_name"]],
            "theme_hits": theme_hits,
            "relevance_reason": "直接命中锚定标的公司层信息",
        }

    if exclude_hits and not strong_theme_hits and not core_hits:
        return {
            "source_level": source_level,
            "relevance_level": "noise",
            "relevance_bucket": "noise",
            "pool_hits": [],
            "theme_hits": theme_hits,
            "relevance_reason": "命中排除语境，且未命中股票池主线",
        }

    if core_hits or strong_theme_hits:
        pool_hits = _unique_keep_order(core_hits + extended_hits)
        return {
            "source_level": source_level,
            "relevance_level": "strong",
            "relevance_bucket": "pool_core",
            "pool_hits": pool_hits,
            "theme_hits": theme_hits,
            "relevance_reason": "命中核心股票池或商业航天强主题",
        }

    if extended_hits or weak_theme_hits:
        pool_hits = _unique_keep_order(extended_hits + core_hits)
        return {
            "source_level": source_level,
            "relevance_level": "weak",
            "relevance_bucket": "pool_extended",
            "pool_hits": pool_hits,
            "theme_hits": theme_hits,
            "relevance_reason": "命中扩展股票池或弱主题，作为背景跟踪",
        }

    return {
        "source_level": source_level,
        "relevance_level": "weak",
        "relevance_bucket": "background",
        "pool_hits": [],
        "theme_hits": theme_hits,
        "relevance_reason": "与主线存在背景交叉，但未命中股票池边界",
    }


def _summarize_relevance_counts(events_list: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        "company_direct_count": 0,
        "pool_core_count": 0,
        "pool_extended_count": 0,
        "background_count": 0,
        "noise_count": 0,
    }
    for event in events_list:
        bucket = event.get("relevance_bucket")
        if bucket == "company_direct":
            counts["company_direct_count"] += 1
        elif bucket == "pool_core":
            counts["pool_core_count"] += 1
        elif bucket == "pool_extended":
            counts["pool_extended_count"] += 1
        elif bucket == "background":
            counts["background_count"] += 1
        elif bucket == "noise":
            counts["noise_count"] += 1
    return counts


def _announcement_matches_anchor(item: Dict[str, Any], anchor_pure_code: str) -> bool:
    code_candidates: List[str] = []
    for field in ("secCode", "secCodeFull"):
        value = item.get(field)
        if value:
            code_candidates.append(str(value))

    sec_code_list = item.get("secCodeList")
    if isinstance(sec_code_list, list):
        code_candidates.extend(str(code) for code in sec_code_list if code)

    normalized = {candidate.split(".")[0] for candidate in code_candidates if candidate}
    if not normalized:
        return False
    return anchor_pure_code in normalized


class AnnouncementProvider:
    """
    公司公告 Provider（巨潮资讯，best-effort）
    """

    BASE_URL = "http://www.cninfo.com.cn/new/fulltextSearch/full"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "http://www.cninfo.com.cn",
    }
    PDF_BASE = "http://static.cninfo.com.cn/"

    def fetch(
        self,
        anchor_name: str,
        anchor_code: str,
        trade_date: str,
    ) -> Tuple[List[Dict[str, Any]], str, Optional[str]]:
        import requests

        date_fmt = _to_date_str(trade_date)
        pure_code = anchor_code.split(".")[0]

        try:
            params = {
                "searchkey": anchor_name,
                "sdate": date_fmt,
                "edate": date_fmt,
                "isfulltext": False,
                "sortName": "time",
                "sortType": "desc",
                "pageNum": 1,
                "pageSize": 20,
            }
            resp = requests.get(
                self.BASE_URL,
                params=params,
                headers=self.HEADERS,
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            raw_list = data.get("announcements") or []
            results = []
            for item in raw_list:
                if not _announcement_matches_anchor(item, pure_code):
                    continue

                title_raw = item.get("announcementTitle", "")
                title = title_raw.replace("<em>", "").replace("</em>", "").strip()
                if not title:
                    continue

                ts_ms = item.get("announcementTime")
                if ts_ms:
                    ann_date = datetime.fromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d")
                else:
                    ann_date = date_fmt

                adjunct = item.get("adjunctUrl", "")
                url = f"{self.PDF_BASE}{adjunct}" if adjunct else ""

                results.append({
                    "title": title,
                    "date": ann_date,
                    "url": url,
                })

            status = "ok" if results else "empty"
            return results, status, None
        except requests.exceptions.Timeout:
            print("[WARN] AnnouncementProvider: 请求超时")
            return [], "timeout", "请求超时"
        except Exception as e:
            print(f"[WARN] AnnouncementProvider: {e}")
            return [], "error", str(e)


class CompanyNewsProvider:
    """
    公司新闻 Provider（东方财富）
    """

    def fetch(
        self,
        anchor_code: str,
        trade_date: str,
        max_items: int = 3,
    ) -> Tuple[List[Dict[str, Any]], str, Optional[str]]:
        import akshare as ak

        pure_code = anchor_code.split(".")[0]
        date_prefix = _to_date_str(trade_date)

        try:
            time.sleep(_REQUEST_INTERVAL)
            df = ak.stock_news_em(symbol=pure_code)

            if df.empty:
                return [], "empty", None

            df_today = df[df["发布时间"].astype(str).str.startswith(date_prefix)]
            if df_today.empty:
                return [], "empty", None

            df_today = df_today.sort_values("发布时间", ascending=False).head(max_items)
            results = [
                {
                    "title": str(row.get("新闻标题", "")).strip(),
                    "datetime": str(row.get("发布时间", "")).strip(),
                    "source": str(row.get("文章来源", "")).strip(),
                }
                for _, row in df_today.iterrows()
                if str(row.get("新闻标题", "")).strip()
            ]
            return results, "ok", None
        except Exception as e:
            print(f"[WARN] CompanyNewsProvider: {e}")
            return [], "error", str(e)


class SectorNewsProvider:
    """
    板块新闻 Provider（东方财富行业新闻，关键词检索）
    """

    _NOISE_EXCLUDE_TERMS = [
        "泡泡玛特", "手办", "玩具", "版权", "IP授权", "盲盒",
        "牙科", "义齿", "骨科", "医疗器械", "种植牙",
        "建筑打印", "房屋打印", "混凝土打印",
        "概念下跌", "概念上涨", "主力资金净流出", "主力资金净流入",
        "融资余额", "排行榜", "龙虎榜",
    ]

    def fetch(
        self,
        sector_keywords: List[str],
        core_codes: List[str],
        trade_date: str,
        max_items: int = 3,
    ) -> Tuple[List[Dict[str, Any]], str, Optional[str]]:
        results, status, error = self._fetch_by_keywords(sector_keywords, trade_date, max_items)
        if status == "ok" and results:
            return results, status, error

        print("[INFO] SectorNewsProvider: 关键词检索无结果，降级为 core_universe 过滤")
        fallback_results, fallback_status, fallback_error = self._fetch_by_core_universe(
            sector_keywords,
            core_codes,
            trade_date,
            max_items,
        )

        if fallback_status == "ok":
            return fallback_results, fallback_status, fallback_error
        if status not in ("ok", "empty") and fallback_status in ("empty", "ok"):
            return fallback_results, "partial", error or fallback_error
        return fallback_results, fallback_status, fallback_error

    def _fetch_by_keywords(
        self,
        sector_keywords: List[str],
        trade_date: str,
        max_items: int,
    ) -> Tuple[List[Dict[str, Any]], str, Optional[str]]:
        import akshare as ak

        date_prefix = _to_date_str(trade_date)
        all_news: List[Dict[str, Any]] = []
        seen_titles = set()
        last_error = None

        for kw in sector_keywords[:4]:
            try:
                time.sleep(_REQUEST_INTERVAL)
                df = ak.stock_news_em(symbol=kw)
                if df.empty:
                    continue

                df_today = df[df["发布时间"].astype(str).str.startswith(date_prefix)]
                if df_today.empty:
                    continue

                for _, row in df_today.iterrows():
                    title = str(row.get("新闻标题", "")).strip()
                    if not title or title in seen_titles or self._is_noise(title, kw):
                        continue

                    seen_titles.add(title)
                    all_news.append({
                        "title": title,
                        "datetime": str(row.get("发布时间", "")).strip(),
                        "source": str(row.get("文章来源", "")).strip(),
                        "matched_keyword": kw,
                    })
            except Exception as e:
                print(f"[WARN] SectorNewsProvider keyword={kw}: {e}")
                last_error = str(e)

        if not all_news:
            return [], "empty", last_error

        all_news.sort(key=lambda x: x["datetime"], reverse=True)
        return all_news[:max_items], "ok", last_error

    def _is_noise(self, title: str, matched_keyword: str) -> bool:
        for term in self._NOISE_EXCLUDE_TERMS:
            if term in title:
                return True

        if matched_keyword in ("增材制造", "3D打印金属", "金属增材"):
            industrial_terms = [
                "航天", "航空", "工业", "金属", "钛合金", "高温合金",
                "铝合金", "不锈钢", "零件", "制造", "成形", "烧结",
            ]
            if not any(term in title for term in industrial_terms):
                return True

        return False

    def _fetch_by_core_universe(
        self,
        sector_keywords: List[str],
        core_codes: List[str],
        trade_date: str,
        max_items: int,
    ) -> Tuple[List[Dict[str, Any]], str, Optional[str]]:
        import akshare as ak

        date_prefix = _to_date_str(trade_date)
        all_news: List[Dict[str, Any]] = []
        seen_titles = set()
        any_success = False
        last_error = None

        for code in core_codes:
            try:
                time.sleep(_REQUEST_INTERVAL)
                df = ak.stock_news_em(symbol=code)
                if df.empty:
                    continue

                any_success = True
                df_today = df[df["发布时间"].astype(str).str.startswith(date_prefix)]
                if df_today.empty:
                    continue

                for _, row in df_today.iterrows():
                    title = str(row.get("新闻标题", "")).strip()
                    if not title or title in seen_titles:
                        continue

                    matched_kw = next((kw for kw in sector_keywords if kw in title), None)
                    if matched_kw is None:
                        continue

                    seen_titles.add(title)
                    all_news.append({
                        "title": title,
                        "datetime": str(row.get("发布时间", "")).strip(),
                        "source": str(row.get("文章来源", "")).strip(),
                        "matched_keyword": matched_kw,
                    })
            except Exception as e:
                print(f"[WARN] SectorNewsProvider core_universe code={code}: {e}")
                last_error = str(e)

        if not any_success:
            return [], "error", last_error or "所有降级来源均失败"
        if not all_news:
            return [], "empty", last_error

        all_news.sort(key=lambda x: x["datetime"], reverse=True)
        return all_news[:max_items], "ok", last_error


def _make_event_id(source_type: str, event_type: str, anchor_code: str, trade_date: str, title: str) -> str:
    title_hash = hashlib.md5(title.encode("utf-8")).hexdigest()[:8]
    code_clean = anchor_code.replace(".", "_")
    return f"{source_type}__{event_type}__{code_clean}__{trade_date}__{title_hash}"


def build_events_list(
    trade_date: str,
    anchor_code: str,
    anchor_name: str,
    announcements: List[Dict[str, Any]],
    announcement_status: str,
    company_news: List[Dict[str, Any]],
    company_news_status: str,
    sector_news: List[Dict[str, Any]],
    sector_news_status: str,
    fetched_at: str,
    analysis_context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    for item in announcements:
        title = item.get("title", "").strip()
        if not title:
            continue
        analysis = _classify_event("announcement", title, analysis_context, [])
        events.append({
            "event_id": _make_event_id("official", "announcement", anchor_code, trade_date, title),
            "event_type": "announcement",
            "source_type": "official",
            "source_level": analysis["source_level"],
            "source_name": "cninfo",
            "title": title,
            "url": item.get("url", ""),
            "published_at": item.get("date", ""),
            "trade_date": trade_date,
            "status": announcement_status,
            "anchor_code": anchor_code,
            "anchor_name": anchor_name,
            "keyword_hits": [],
            "theme_hits": analysis["theme_hits"],
            "relevance_level": analysis["relevance_level"],
            "relevance_bucket": analysis["relevance_bucket"],
            "pool_hits": analysis["pool_hits"],
            "relevance_reason": analysis["relevance_reason"],
            "fetched_at": fetched_at,
        })

    for item in company_news:
        title = item.get("title", "").strip()
        if not title:
            continue
        analysis = _classify_event("company_news", title, analysis_context, [])
        events.append({
            "event_id": _make_event_id("aggregator", "company_news", anchor_code, trade_date, title),
            "event_type": "company_news",
            "source_type": "aggregator",
            "source_level": analysis["source_level"],
            "source_name": "eastmoney",
            "title": title,
            "url": item.get("url", ""),
            "published_at": item.get("datetime", ""),
            "trade_date": trade_date,
            "status": company_news_status,
            "anchor_code": anchor_code,
            "anchor_name": anchor_name,
            "keyword_hits": [],
            "theme_hits": analysis["theme_hits"],
            "relevance_level": analysis["relevance_level"],
            "relevance_bucket": analysis["relevance_bucket"],
            "pool_hits": analysis["pool_hits"],
            "relevance_reason": analysis["relevance_reason"],
            "fetched_at": fetched_at,
        })

    for item in sector_news:
        title = item.get("title", "").strip()
        if not title:
            continue

        keyword_hits = item.get("matched_keyword", [])
        if isinstance(keyword_hits, str):
            keyword_hits = [keyword_hits]
        elif not isinstance(keyword_hits, list):
            keyword_hits = []
        keyword_hits = _unique_keep_order(keyword_hits)
        analysis = _classify_event("sector_news", title, analysis_context, keyword_hits)

        events.append({
            "event_id": _make_event_id("aggregator", "sector_news", anchor_code, trade_date, title),
            "event_type": "sector_news",
            "source_type": "aggregator",
            "source_level": analysis["source_level"],
            "source_name": "eastmoney",
            "title": title,
            "url": item.get("url", ""),
            "published_at": item.get("datetime", ""),
            "trade_date": trade_date,
            "status": sector_news_status,
            "anchor_code": anchor_code,
            "anchor_name": anchor_name,
            "keyword_hits": keyword_hits,
            "theme_hits": analysis["theme_hits"],
            "relevance_level": analysis["relevance_level"],
            "relevance_bucket": analysis["relevance_bucket"],
            "pool_hits": analysis["pool_hits"],
            "relevance_reason": analysis["relevance_reason"],
            "fetched_at": fetched_at,
        })

    return events


def derive_overall_status(provider_statuses: Dict[str, Dict[str, Any]]) -> str:
    statuses = [payload.get("status", "error") for payload in provider_statuses.values()]
    if all(status == "empty" for status in statuses):
        return "empty"
    if all(status in ("ok", "empty") for status in statuses):
        return "ok"
    if any(status in ("ok", "empty") for status in statuses):
        return "partial"
    return "error"


def compute_event_signal_label(
    announcements: List[Dict[str, Any]],
    announcement_status: str,
    company_news: List[Dict[str, Any]],
    company_news_status: str,
    sector_news: List[Dict[str, Any]],
    sector_news_status: str,
    relevance_counts: Optional[Dict[str, int]] = None,
) -> str:
    relevance_counts = relevance_counts or {}
    n_ann = len(announcements) if announcement_status == "ok" else 0
    n_company = len(company_news) if company_news_status == "ok" else 0
    n_core = relevance_counts.get("pool_core_count", 0)
    has_fetch_issue = any(
        status not in ("ok", "empty")
        for status in (announcement_status, company_news_status, sector_news_status)
    )

    if n_ann >= 1 or n_company >= 2:
        return "有明确催化"
    if n_company == 1 or n_core >= 1:
        return "有弱催化"
    if has_fetch_issue:
        return "信息不足"
    return "无明确催化"


def build_event_summary(
    trade_date: str,
    anchor_name: str,
    announcements: List[Dict[str, Any]],
    announcement_status: str,
    company_news: List[Dict[str, Any]],
    company_news_status: str,
    sector_news: List[Dict[str, Any]],
    sector_news_status: str,
    signal_label: str,
    relevance_counts: Optional[Dict[str, int]] = None,
) -> str:
    relevance_counts = relevance_counts or {}
    if announcement_status == "ok":
        company_part = f"{len(announcements)} 条{anchor_name}公告" if announcements else f"无{anchor_name}公告"
    elif announcement_status == "empty":
        company_part = f"无{anchor_name}公告"
    elif company_news_status == "ok" and company_news:
        company_part = f"{len(company_news)} 条公司新闻"
    elif company_news_status == "empty":
        company_part = "无公司新闻"
    else:
        company_part = (
            f"公告未获取（{announcement_status}）"
            if announcement_status not in ("ok", "empty")
            else f"公司新闻未获取（{company_news_status}）"
        )

    core_count = relevance_counts.get("pool_core_count", 0)
    extended_count = relevance_counts.get("pool_extended_count", 0)
    background_count = relevance_counts.get("background_count", 0)
    noise_count = relevance_counts.get("noise_count", 0)

    if sector_news_status not in ("ok", "empty"):
        core_part = f"板块新闻未获取（{sector_news_status}）"
    elif core_count > 0:
        core_part = f"{core_count} 条核心板块相关信息"
    else:
        core_part = "未见明确核心板块催化"

    if extended_count > 0:
        background_part = f"{extended_count} 条扩展池/弱主题信息"
    elif background_count > 0:
        background_part = f"{background_count} 条背景信息"
    elif noise_count > 0:
        background_part = f"{noise_count} 条噪音信息已降级"
    else:
        background_part = "无额外背景补充"

    if company_news_status not in ("ok", "empty") and announcement_status in ("ok", "empty"):
        company_part = f"{company_part}，公司新闻未获取（{company_news_status}）"

    return (
        f"{trade_date}：公司层{company_part}；"
        f"核心板块层{core_part}；"
        f"背景层{background_part}；"
        f"初步判断：{signal_label}"
    )


def _build_provider_statuses(
    announcement_status: str,
    announcement_error: Optional[str],
    announcements: List[Dict[str, Any]],
    company_news_status: str,
    company_news_error: Optional[str],
    company_news: List[Dict[str, Any]],
    sector_news_status: str,
    sector_news_error: Optional[str],
    sector_news: List[Dict[str, Any]],
    fetched_at: str,
) -> Dict[str, Dict[str, Any]]:
    return {
        "announcement": {
            "status": announcement_status,
            "error": announcement_error,
            "item_count": len(announcements),
            "fetched_at": fetched_at,
        },
        "company_news": {
            "status": company_news_status,
            "error": company_news_error,
            "item_count": len(company_news),
            "fetched_at": fetched_at,
        },
        "sector_news": {
            "status": sector_news_status,
            "error": sector_news_error,
            "item_count": len(sector_news),
            "fetched_at": fetched_at,
        },
    }


def _combine_errors(provider_statuses: Dict[str, Dict[str, Any]]) -> Optional[str]:
    errors = []
    for name, payload in provider_statuses.items():
        error = payload.get("error")
        if error:
            errors.append(f"{name}={error}")
    return " | ".join(errors) if errors else None


def _default_sector_keywords() -> List[str]:
    return [
        "商业航天", "增材制造", "金属增材", "3D打印金属",
        "航天发动机", "运载火箭", "卫星互联网", "低轨卫星",
        "航天电子", "航天动力", "航天工程",
        "航天强国", "商业发射",
    ]


def _empty_result(
    trade_date: str,
    anchor_code: str,
    anchor_name: str,
    error: str,
) -> Dict[str, Any]:
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    provider_statuses = {
        "announcement": {"status": "error", "error": error, "item_count": 0, "fetched_at": generated_at},
        "company_news": {"status": "error", "error": error, "item_count": 0, "fetched_at": generated_at},
        "sector_news": {"status": "error", "error": error, "item_count": 0, "fetched_at": generated_at},
    }
    return {
        "latest_trade_date": trade_date,
        "trade_date": trade_date,
        "anchor_code": anchor_code,
        "anchor_name": anchor_name,
        "provider_statuses": provider_statuses,
        "overall_status": "error",
        "company_announcements": [],
        "announcement_status": "error",
        "company_news": [],
        "company_news_status": "error",
        "sector_news": [],
        "sector_news_status": "error",
        "event_signal_label": "信息不足",
        "event_summary": f"{trade_date}：事件层初始化失败（{error}）",
        "events_list": [],
        "error": error,
        "generated_at": generated_at,
    }


def _save_events(result: Dict[str, Any], output_path: str) -> None:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    trade_date = result.get("latest_trade_date") or result.get("trade_date")
    if trade_date:
        ARCHIVE_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
        archive_file = ARCHIVE_EVENTS_DIR / f"{trade_date}.json"
        with open(archive_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)


def collect_events(
    trade_date: str,
    anchor_code: str,
    anchor_name: str,
    config_path: str = None,
    output_path: str = None,
) -> Dict[str, Any]:
    if output_path is None:
        output_path = str(DEFAULT_EVENTS_OUTPUT)

    try:
        config = load_config(config_path)
    except Exception as e:
        result = _empty_result(trade_date, anchor_code, anchor_name, str(e))
        _save_events(result, output_path)
        return result

    try:
        from news_sources import sync_news_sources_registry

        news_sources_registry = sync_news_sources_registry()
    except Exception as e:
        print(f"[WARN] 新闻源 registry 刷新失败: {e}")
        news_sources_registry = None

    core_universe = config.get("core_universe", [])
    core_codes = [item["code"].split(".")[0] for item in core_universe if item.get("code")]
    event_cfg = config.get("event_keywords", {})
    sector_keywords = event_cfg.get("sector", _default_sector_keywords())
    analysis_context = _build_news_analysis_context(config, anchor_code, anchor_name)

    print(f"[INFO] 新闻数据链 v1.0：获取 {trade_date} 的事件数据...")

    ann_provider = AnnouncementProvider()
    print("[INFO] AnnouncementProvider: 获取公司公告...")
    announcements, announcement_status, announcement_error = ann_provider.fetch(anchor_name, anchor_code, trade_date)
    print(f"[INFO] 公告: status={announcement_status}, 条数={len(announcements)}")

    co_provider = CompanyNewsProvider()
    print("[INFO] CompanyNewsProvider: 获取公司新闻...")
    company_news, company_news_status, company_news_error = co_provider.fetch(anchor_code, trade_date)
    print(f"[INFO] 公司新闻: status={company_news_status}, 条数={len(company_news)}")

    sec_provider = SectorNewsProvider()
    print("[INFO] SectorNewsProvider: 获取板块新闻...")
    sector_news, sector_news_status, sector_news_error = sec_provider.fetch(
        sector_keywords,
        core_codes,
        trade_date,
    )
    print(f"[INFO] 板块新闻: status={sector_news_status}, 条数={len(sector_news)}")

    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    provider_statuses = _build_provider_statuses(
        announcement_status,
        announcement_error,
        announcements,
        company_news_status,
        company_news_error,
        company_news,
        sector_news_status,
        sector_news_error,
        sector_news,
        fetched_at,
    )
    overall_status = derive_overall_status(provider_statuses)

    events_list = build_events_list(
        trade_date,
        anchor_code,
        anchor_name,
        announcements,
        announcement_status,
        company_news,
        company_news_status,
        sector_news,
        sector_news_status,
        fetched_at,
        analysis_context,
    )
    relevance_counts = _summarize_relevance_counts(events_list)
    signal_label = compute_event_signal_label(
        announcements,
        announcement_status,
        company_news,
        company_news_status,
        sector_news,
        sector_news_status,
        relevance_counts,
    )
    event_summary = build_event_summary(
        trade_date,
        anchor_name,
        announcements,
        announcement_status,
        company_news,
        company_news_status,
        sector_news,
        sector_news_status,
        signal_label,
        relevance_counts,
    )

    result = {
        "latest_trade_date": trade_date,
        "trade_date": trade_date,
        "anchor_code": anchor_code,
        "anchor_name": anchor_name,
        "analysis_context": {
            "strong_themes": analysis_context["strong_themes"],
            "weak_themes": analysis_context["weak_themes"],
            "core_names": analysis_context["core_names"],
            "extended_names": analysis_context["extended_names"],
        },
        "news_sources_registry_count": 0 if news_sources_registry is None else news_sources_registry.get("source_count", 0),
        "provider_statuses": provider_statuses,
        "overall_status": overall_status,
        "company_announcements": announcements,
        "announcement_status": announcement_status,
        "company_news": company_news,
        "company_news_status": company_news_status,
        "sector_news": sector_news,
        "sector_news_status": sector_news_status,
        "event_signal_label": signal_label,
        "event_summary": event_summary,
        "events_list": events_list,
        **relevance_counts,
        "error": _combine_errors(provider_statuses),
        "generated_at": fetched_at,
    }

    _save_events(result, output_path)
    print(f"[INFO] 事件数据已保存至: {output_path}")
    print(f"[INFO] 事件摘要: {event_summary}")
    return result


def load_events(events_path: str = None) -> Optional[Dict[str, Any]]:
    events_file = Path(events_path) if events_path else DEFAULT_EVENTS_OUTPUT
    if not events_file.exists():
        return None

    try:
        with open(events_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] 加载事件数据失败: {e}")
        return None


def load_news_data_product(events_path: str = None) -> Optional[Dict[str, Any]]:
    return load_events(events_path)


def main():
    import pandas as pd

    metrics_path = DATA_PROCESSED_DIR / "daily_metrics.parquet"
    if not metrics_path.exists():
        print("[ERROR] 请先运行 analyzer 生成 daily_metrics.parquet")
        return

    df = pd.read_parquet(metrics_path)
    latest = df.sort_values("trade_date", ascending=False).iloc[0]
    trade_date = pd.Timestamp(latest["trade_date"]).strftime("%Y%m%d")
    anchor_code = latest["anchor_symbol"]

    config = load_config()
    anchor_name = config["anchor"]["name"]

    print(f"交易日期: {trade_date}, 标的: {anchor_name}（{anchor_code}）")
    result = collect_events(trade_date, anchor_code, anchor_name)

    print("\n=== 新闻数据产品结果 ===")
    print(f"整体状态: {result['overall_status']}")
    print(f"信号: {result['event_signal_label']}")
    print(f"摘要: {result['event_summary']}")
    print(f"公告: {len(result['company_announcements'])} 条 [{result['announcement_status']}]")
    print(f"公司新闻: {len(result['company_news'])} 条 [{result['company_news_status']}]")
    print(f"板块新闻: {len(result['sector_news'])} 条 [{result['sector_news_status']}]")


if __name__ == "__main__":
    main()
