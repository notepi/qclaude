"""
极简新闻源入口模块。

职责：
  - 读取 config/news_sources.yaml 中的纯链接列表
  - 自动识别来源等级、来源类型和抓取模式
  - 生成 data/processed/news_sources_registry.json
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import yaml

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "news_sources.yaml"
REGISTRY_OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "news_sources_registry.json"


def _utc_now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    normalized = f"{scheme}://{netloc}{path}"
    if parsed.query:
        normalized = f"{normalized}?{parsed.query}"
    return normalized


def _is_valid_url(url: str) -> bool:
    parsed = urlparse(url.strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def load_news_sources_config(config_path: str = None) -> List[str]:
    path = Path(config_path) if config_path else CONFIG_PATH
    if not path.exists():
        return []

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    raw_sources = data.get("sources", [])
    if not isinstance(raw_sources, list):
        raise ValueError("news_sources.yaml 中的 sources 必须为列表")

    seen = set()
    normalized_sources = []
    for item in raw_sources:
        if not isinstance(item, str):
            continue
        url = item.strip()
        if not url or not _is_valid_url(url):
            continue
        normalized = _normalize_url(url)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_sources.append(url)

    return normalized_sources


def classify_news_source(url: str) -> Dict[str, Any]:
    normalized_url = _normalize_url(url)
    parsed = urlparse(normalized_url)
    domain = parsed.netloc.lower()
    path = parsed.path.lower()

    source_name = domain
    source_level = "unknown"
    source_kind = "unknown"
    fetch_mode = "unsupported"
    adapter_name = "unsupported"
    reason = "未命中已知规则，保留为待识别来源"

    if domain == "tushare.pro" and path == "/news/cls":
        source_level = "L2"
        source_kind = "tushare_news_portal"
        fetch_mode = "browser_session"
        adapter_name = "tushare_cls"
        source_name = "tushare_cls"
        reason = "命中 Tushare 财联社新闻入口规则"
    elif domain == "tushare.pro" and path == "/news/eastmoney":
        source_level = "L2"
        source_kind = "tushare_news_portal"
        fetch_mode = "browser_session"
        adapter_name = "tushare_eastmoney"
        source_name = "tushare_eastmoney"
        reason = "命中 Tushare 东方财富新闻入口规则"
    elif "cninfo.com.cn" in domain or "sse.com.cn" in domain or "szse.cn" in domain:
        source_level = "L1"
        source_kind = "company_official"
        fetch_mode = "html_list"
        adapter_name = "html_list"
        source_name = "cninfo" if "cninfo.com.cn" in domain else domain
        reason = "命中公告/交易所官方来源规则"
    elif path.endswith(".xml") or "rss" in path:
        source_level = "L2"
        source_kind = "media_feed"
        fetch_mode = "rss"
        adapter_name = "rss"
        reason = "URL 路径命中 RSS 规则"
    elif "eastmoney.com" in domain or "jrj.com.cn" in domain or "stcn.com" in domain or "cls.cn" in domain:
        source_level = "L2"
        source_kind = "media_aggregator"
        fetch_mode = "json_api" if "search-api-web.eastmoney.com" in domain else "html_list"
        adapter_name = fetch_mode
        source_name = "eastmoney" if "eastmoney.com" in domain else domain
        reason = "命中财经媒体/聚合来源规则"

    return {
        "url": url,
        "normalized_url": normalized_url,
        "domain": domain,
        "source_name": source_name,
        "source_level": source_level,
        "source_kind": source_kind,
        "fetch_mode": fetch_mode,
        "adapter_name": adapter_name,
        "enabled": True,
        "last_checked_at": _utc_now_str(),
        "classification_reason": reason,
    }


def build_news_sources_registry(urls: List[str]) -> Dict[str, Any]:
    entries = [classify_news_source(url) for url in urls]
    return {
        "generated_at": _utc_now_str(),
        "source_count": len(entries),
        "sources": entries,
    }


def save_news_sources_registry(registry: Dict[str, Any], output_path: str = None) -> str:
    path = Path(output_path) if output_path else REGISTRY_OUTPUT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registry, f, ensure_ascii=False, indent=2)
    return str(path)


def load_news_sources_registry(path: str = None) -> Optional[Dict[str, Any]]:
    registry_path = Path(path) if path else REGISTRY_OUTPUT_PATH
    if not registry_path.exists():
        return None
    with open(registry_path, "r", encoding="utf-8") as f:
        return json.load(f)


def sync_news_sources_registry(config_path: str = None, output_path: str = None) -> Dict[str, Any]:
    urls = load_news_sources_config(config_path)
    registry = build_news_sources_registry(urls)
    save_news_sources_registry(registry, output_path)
    return registry
