"""
新闻源抓取层。

职责：
  - 读取 news_sources registry
  - 对 browser_session 类型源执行定制适配器
  - 保存原始抓取结果到 data/raw/news_sources/
  - 提供统一标准化入口，供后续事件层接入
"""

import json
import re
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).parent.parent
RAW_NEWS_SOURCES_DIR = PROJECT_ROOT / "data" / "raw" / "news_sources"
LATEST_OUTPUT_PATH = RAW_NEWS_SOURCES_DIR / "latest.json"
EDGE_CDP_ENDPOINT = "http://127.0.0.1:9222"
DEDICATED_EDGE_PROFILE_ROOT = Path.home() / ".edge-codex-debug"
DEDICATED_EDGE_PROFILE_NAME = "Default"


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _slugify_source_name(source_name: str) -> str:
    return source_name.lower().replace(" ", "_").replace("/", "_")


def _has_playwright() -> bool:
    try:
        import playwright  # noqa: F401

        return True
    except Exception:
        return False


def _cdp_available(endpoint: str = EDGE_CDP_ENDPOINT) -> bool:
    try:
        import urllib.request

        with urllib.request.urlopen(f"{endpoint}/json/version", timeout=2) as resp:
            return resp.status == 200
    except Exception:
        return False


def _dedicated_edge_launch_command() -> str:
    return (
        'open -na "Microsoft Edge" --args '
        f'--remote-debugging-port=9222 --user-data-dir="{DEDICATED_EDGE_PROFILE_ROOT}"'
    )


def _find_browser_session() -> Optional[Dict[str, str]]:
    candidates = [
        {
            "browser_name": "msedge",
            "profile_root": str(DEDICATED_EDGE_PROFILE_ROOT),
            "profile_name": DEDICATED_EDGE_PROFILE_NAME,
        },
        {
            "browser_name": "msedge",
            "profile_root": str(Path.home() / "Library/Application Support/Microsoft Edge"),
            "profile_name": "Default",
        },
        {
            "browser_name": "chrome",
            "profile_root": str(Path.home() / "Library/Application Support/Google/Chrome"),
            "profile_name": "Default",
        },
        {
            "browser_name": "chromium",
            "profile_root": str(Path.home() / "Library/Application Support/Chromium"),
            "profile_name": "Default",
        },
        {
            "browser_name": "brave",
            "profile_root": str(Path.home() / "Library/Application Support/BraveSoftware/Brave-Browser"),
            "profile_name": "Default",
        },
    ]
    for candidate in candidates:
        profile_root = Path(candidate["profile_root"])
        profile_path = profile_root / candidate["profile_name"]
        if profile_path.exists():
            return {
                **candidate,
                "profile_path": str(profile_path),
            }
    return None


def _browser_session_unavailable_result(source: Dict[str, Any], status: str, reason: str) -> Dict[str, Any]:
    return {
        "source_url": source["url"],
        "source_name": source["source_name"],
        "source_level": source.get("source_level"),
        "source_kind": source.get("source_kind"),
        "adapter_name": source.get("adapter_name"),
        "fetch_mode": source.get("fetch_mode"),
        "browser_name": None,
        "profile_path": None,
        "status": status,
        "error": reason,
        "fetched_at": _now_str(),
        "items": [],
    }


def _auth_required_result(source: Dict[str, Any], browser_name: Optional[str], profile_path: Optional[str], reason: str) -> Dict[str, Any]:
    return {
        "source_url": source["url"],
        "source_name": source["source_name"],
        "source_level": source.get("source_level"),
        "source_kind": source.get("source_kind"),
        "adapter_name": source.get("adapter_name"),
        "fetch_mode": source.get("fetch_mode"),
        "browser_name": browser_name,
        "profile_path": profile_path,
        "status": "auth_required",
        "error": reason,
        "fetched_at": _now_str(),
        "items": [],
    }


def _parse_tushare_news_items(body_text: str, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    items: List[Dict[str, Any]] = []
    time_pattern = re.compile(r"^\d{2}:\d{2}$")
    ignore_lines = {
        "首页", "平台介绍", "数据接口", "资讯数据", "数据工具", "社区捐助",
        "搜索", "公司", "加红", "港美股", "基金", "看盘", "提醒",
        "雪球", "第一财经", "凤凰", "同花顺", "金融界", "新浪财经", "云财经", "财联社", "东方财富", "华尔街见闻",
    }

    i = 0
    while i < len(lines):
        line = lines[i]
        if line in ignore_lines:
            i += 1
            continue
        if time_pattern.match(line) and i + 1 < len(lines):
            next_line = lines[i + 1]
            if next_line not in ignore_lines and len(next_line) > 8:
                items.append({
                    "title": next_line[:120],
                    "url": source["url"],
                    "published_at": next_line[:5] if next_line[:5].count(":") == 1 else line,
                    "summary": next_line,
                })
                i += 2
                continue
        i += 1

    return items


def _extract_items_from_connected_page(page, source: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        body_text = page.locator("body").inner_text()
    except Exception:
        return []

    if source.get("source_kind") == "tushare_news_portal":
        return _parse_tushare_news_items(body_text, source)
    return []


def _choose_cdp_page(context, source: Dict[str, Any]):
    pages = list(context.pages)
    target_url = source["url"]

    for page in pages:
        if page.url == target_url:
            return page, False

    for page in pages:
        if page.url.startswith("https://tushare.pro/news/"):
            return page, True

    for page in pages:
        if page.url.startswith("https://tushare.pro/"):
            return page, True

    for page in pages:
        if not page.url.startswith(("edge://", "chrome-extension://", "devtools://")):
            return page, True

    if pages:
        return pages[0], True

    return context.new_page(), True


def _fetch_via_cdp_session(source: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not _cdp_available():
        return None

    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(EDGE_CDP_ENDPOINT)
            if not browser.contexts:
                browser.close()
                return _browser_session_unavailable_result(source, "render_failed", "CDP 已连接，但未发现可用浏览器上下文")

            context = browser.contexts[0]
            page, should_navigate = _choose_cdp_page(context, source)
            if should_navigate:
                page.goto(source["url"], wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(4000)

            items = _extract_items_from_connected_page(page, source)
            if items:
                browser.close()
                return {
                    "source_url": source["url"],
                    "source_name": source["source_name"],
                    "source_level": source.get("source_level"),
                    "source_kind": source.get("source_kind"),
                    "adapter_name": source.get("adapter_name"),
                    "fetch_mode": source.get("fetch_mode"),
                    "browser_name": "msedge_cdp",
                    "profile_path": "remote-debugging-session",
                    "status": "ok",
                    "error": None,
                    "fetched_at": _now_str(),
                    "items": items,
                }

            if _is_login_page(page):
                browser.close()
                return {
                    **_auth_required_result(
                        source,
                        "msedge_cdp",
                        "remote-debugging-session",
                        "已连接运行中的 Edge 会话，但页面仍处于登录页；如果要持久复用登录态，请使用专用浏览器目录并重新登录一次："
                        f" {_dedicated_edge_launch_command()}",
                    )
                }

            browser.close()
            return {
                "source_url": source["url"],
                "source_name": source["source_name"],
                "source_level": source.get("source_level"),
                "source_kind": source.get("source_kind"),
                "adapter_name": source.get("adapter_name"),
                "fetch_mode": source.get("fetch_mode"),
                "browser_name": "msedge_cdp",
                "profile_path": "remote-debugging-session",
                "status": "render_failed",
                "error": "已连接登录会话，但未能从页面正文解析出新闻列表",
                "fetched_at": _now_str(),
                "items": [],
            }
    except Exception as e:
        return {
            "source_url": source["url"],
            "source_name": source["source_name"],
            "source_level": source.get("source_level"),
            "source_kind": source.get("source_kind"),
            "adapter_name": source.get("adapter_name"),
            "fetch_mode": source.get("fetch_mode"),
            "browser_name": "msedge_cdp",
            "profile_path": "remote-debugging-session",
            "status": "error",
            "error": str(e),
            "fetched_at": _now_str(),
            "items": [],
        }


def _copy_browser_profile(session: Dict[str, str]) -> Path:
    profile_root = Path(session["profile_root"])
    profile_name = session["profile_name"]
    src_profile = profile_root / profile_name
    tmp_root = Path(tempfile.mkdtemp(prefix=f"{session['browser_name']}-playwright-"))
    (tmp_root / profile_name).mkdir(parents=True, exist_ok=True)

    local_state = profile_root / "Local State"
    if local_state.exists():
        shutil.copy2(local_state, tmp_root / "Local State")

    for name in [
        "Cookies",
        "Network",
        "Preferences",
        "Web Data",
        "Login Data",
        "Local Storage",
        "Session Storage",
        "IndexedDB",
    ]:
        src = src_profile / name
        if not src.exists():
            continue
        dst = tmp_root / profile_name / name
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)

    return tmp_root


def _is_login_page(page) -> bool:
    try:
        url = page.url.lower()
        title = page.title().lower()
        content = page.content().lower()
    except Exception:
        return False

    explicit_markers = (
        "wxlogin",
        "扫码登录",
        "微信登录",
        "手机登录",
        "账号登录",
        "登录后查看",
    )

    return (
        "#/login" in url
        or "/login" in url
        or any(marker in content for marker in explicit_markers)
        or "tushare数据" in title and "#/login" in url
    )


def _fetch_with_browser_session(source: Dict[str, Any]) -> Dict[str, Any]:
    cdp_result = _fetch_via_cdp_session(source)
    if cdp_result is not None:
        return cdp_result

    session = _find_browser_session()
    if session is None:
        return _browser_session_unavailable_result(
            source,
            "auth_required",
            "未发现可复用的本机浏览器登录态目录。建议先启动专用抓取浏览器并登录一次："
            f" {_dedicated_edge_launch_command()}",
        )

    if not _has_playwright():
        return _browser_session_unavailable_result(
            source,
            "render_failed",
            "当前环境未安装 playwright，无法复用浏览器会话渲染页面",
        )

    try:
        from playwright.sync_api import sync_playwright

        tmp_root = _copy_browser_profile(session)
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir=str(tmp_root),
                channel=session["browser_name"],
                headless=True,
                args=[f"--profile-directory={session['profile_name']}"],
            )
            page = context.new_page()
            page.goto(source["url"], wait_until="domcontentloaded", timeout=20000)
            page.wait_for_timeout(2000)

            if _is_login_page(page):
                context.close()
                return {
                    **_auth_required_result(
                        source,
                        session["browser_name"],
                        session["profile_path"],
                        "已复用浏览器 profile，但页面仍跳转到登录页，当前 profile 中没有可用的 Tushare 登录态。"
                        f" 建议优先使用专用抓取 profile：{DEDICATED_EDGE_PROFILE_ROOT}",
                    )
                }

            context.close()
            return {
                "source_url": source["url"],
                "source_name": source["source_name"],
                "source_level": source.get("source_level"),
                "source_kind": source.get("source_kind"),
                "adapter_name": source.get("adapter_name"),
                "fetch_mode": source.get("fetch_mode"),
                "browser_name": session["browser_name"],
                "profile_path": session["profile_path"],
                "status": "render_failed",
                "error": "浏览器会话已打开页面，但新闻列表解析适配器尚未完成",
                "fetched_at": _now_str(),
                "items": [],
            }
    except Exception as e:
        return {
            "source_url": source["url"],
            "source_name": source["source_name"],
            "source_level": source.get("source_level"),
            "source_kind": source.get("source_kind"),
            "adapter_name": source.get("adapter_name"),
            "fetch_mode": source.get("fetch_mode"),
            "browser_name": session["browser_name"],
            "profile_path": session["profile_path"],
            "status": "error",
            "error": str(e),
            "fetched_at": _now_str(),
            "items": [],
        }


def fetch_registered_source(source: Dict[str, Any]) -> Dict[str, Any]:
    fetch_mode = source.get("fetch_mode")
    if fetch_mode == "browser_session":
        return _fetch_with_browser_session(source)

    return {
        "source_url": source["url"],
        "source_name": source["source_name"],
        "source_level": source.get("source_level"),
        "source_kind": source.get("source_kind"),
        "adapter_name": source.get("adapter_name"),
        "fetch_mode": fetch_mode,
        "status": "unsupported",
        "error": f"当前未实现 {fetch_mode} 抓取器",
        "fetched_at": _now_str(),
        "items": [],
    }


def save_raw_fetch_results(results: List[Dict[str, Any]], output_dir: str = None) -> Dict[str, str]:
    base_dir = Path(output_dir) if output_dir else RAW_NEWS_SOURCES_DIR
    base_dir.mkdir(parents=True, exist_ok=True)

    latest_payload = {
        "generated_at": _now_str(),
        "source_count": len(results),
        "results": results,
    }
    with open(base_dir / "latest.json", "w", encoding="utf-8") as f:
        json.dump(latest_payload, f, ensure_ascii=False, indent=2)

    saved_files = {"latest": str(base_dir / "latest.json")}
    for result in results:
        slug = _slugify_source_name(result["source_name"])
        path = base_dir / f"{slug}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        saved_files[slug] = str(path)

    return saved_files


def fetch_configured_news_sources(config_path: str = None, registry_path: str = None, output_dir: str = None) -> Dict[str, Any]:
    from news_sources import load_news_sources_registry, sync_news_sources_registry

    registry = load_news_sources_registry(registry_path)
    if registry is None:
        registry = sync_news_sources_registry(config_path=config_path)

    results = []
    for source in registry.get("sources", []):
        if not source.get("enabled", True):
            continue
        results.append(fetch_registered_source(source))

    saved_files = save_raw_fetch_results(results, output_dir=output_dir)
    return {
        "generated_at": _now_str(),
        "source_count": len(results),
        "results": results,
        "saved_files": saved_files,
    }


def normalize_fetched_source_items(fetch_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    normalized_items = []
    for source_result in fetch_result.get("results", []):
        for item in source_result.get("items", []):
            normalized_items.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "published_at": item.get("published_at", ""),
                "summary": item.get("summary", ""),
                "source_name": source_result.get("source_name", ""),
                "source_url": source_result.get("source_url", ""),
                "fetched_at": source_result.get("fetched_at", ""),
            })
    return normalized_items
