"""
新闻催化分析模块。

复用现有的新闻抓取系统，添加关键词筛选和 AI 智能筛选功能。
"""

import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import yaml

# 复用现有的新闻抓取模块
import sys
sys.path.insert(0, str(Path(__file__).parent))

from news_sources import sync_news_sources_registry, load_news_sources_registry
from news_source_fetcher import fetch_configured_news_sources, normalize_fetched_source_items
from ai_filter import filter_news_with_ai, check_ai_filter_available

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data" / "catalyst" / "news"

RULES_PATH = CONFIG_DIR / "catalyst_rules.yaml"


def load_catalyst_rules() -> Dict[str, List[str]]:
    """加载筛选规则"""
    if not RULES_PATH.exists():
        return {"include_keywords": [], "exclude_keywords": []}

    with open(RULES_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def generate_news_id(title: str, source: str, published_at: str) -> str:
    """生成新闻唯一ID"""
    content = f"{source}_{title}_{published_at}"
    hash_val = hashlib.md5(content.encode()).hexdigest()[:8]
    date_part = datetime.now().strftime("%Y%m%d")
    return f"news_{date_part}_{hash_val}"


def filter_by_keywords(items: List[Dict[str, Any]], rules: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """根据关键词筛选新闻"""
    include_keywords = rules.get("include_keywords", [])
    exclude_keywords = rules.get("exclude_keywords", [])

    filtered = []
    for item in items:
        title = item.get("title", "")
        summary = item.get("summary", "")
        text = f"{title} {summary}"

        # 检查排除关键词
        excluded = False
        for kw in exclude_keywords:
            if kw in text:
                excluded = True
                break

        if excluded:
            continue

        # 检查包含关键词
        matched = []
        for kw in include_keywords:
            if kw in text:
                matched.append(kw)

        if matched:
            item["matched_keywords"] = matched
            item["news_id"] = generate_news_id(title, item.get("source_name", ""), item.get("published_at", ""))
            filtered.append(item)

    return filtered


def save_catalyst_news(items: List[Dict[str, Any]], total_items: int, source_count: int) -> Path:
    """保存筛选后的新闻"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    output_path = DATA_DIR / f"{today}.json"

    # 如果已有文件，合并数据
    existing_items = {}
    if output_path.exists():
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            for item in data.get("items", []):
                existing_items[item.get("news_id", "")] = item

    # 合并新数据
    for item in items:
        news_id = item.get("news_id", "")
        if news_id:
            existing_items[news_id] = item

    final_items = list(existing_items.values())

    result = {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "source_count": source_count,
        "total_items": total_items,
        "filtered_items": len(final_items),
        "items": final_items,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"结果已保存到: {output_path}")
    print(f"共 {len(final_items)} 条筛选后新闻（去重后）")

    return output_path


def run_catalyst_collector(additional_urls: List[str] = None) -> Dict[str, Any]:
    """
    主入口：采集新闻并筛选保存。

    Args:
        additional_urls: 可选的额外新闻源 URL 列表

    Returns:
        包含统计信息和保存路径的字典
    """
    # 1. 同步新闻源注册表
    print("=" * 50)
    print("新闻催化分析系统")
    print("=" * 50)

    registry = sync_news_sources_registry()
    source_count = registry.get("source_count", 0)
    print(f"已注册 {source_count} 个新闻源")

    # 2. 加载筛选规则
    rules = load_catalyst_rules()
    include_count = len(rules.get("include_keywords", []))
    exclude_count = len(rules.get("exclude_keywords", []))
    print(f"筛选规则: {include_count} 个包含关键词, {exclude_count} 个排除关键词")

    # 检查 AI 筛选是否可用
    ai_enabled = rules.get("ai_filter", {}).get("enabled", True) and check_ai_filter_available()
    if ai_enabled:
        print("AI 智能筛选: 已启用")
    else:
        print("AI 智能筛选: 未启用或不可用")
    print("-" * 50)

    # 3. 抓取新闻
    print("开始抓取新闻...")
    fetch_result = fetch_configured_news_sources()

    # 4. 标准化并筛选
    all_items = normalize_fetched_source_items(fetch_result)
    total_items = len(all_items)
    print(f"共获取 {total_items} 条新闻")

    # 5. 关键词粗筛
    keyword_filtered = filter_by_keywords(all_items, rules)
    print(f"关键词粗筛后 {len(keyword_filtered)} 条新闻")

    # 6. AI 精筛（如果启用）
    final_items = keyword_filtered
    ai_filtered_count = len(keyword_filtered)
    if ai_enabled and keyword_filtered:
        final_items = filter_news_with_ai(keyword_filtered, rules)
        ai_filtered_count = len(final_items)
        print(f"AI 精筛后 {ai_filtered_count} 条新闻")

    # 7. 保存结果
    output_path = save_catalyst_news(final_items, total_items, source_count)

    return {
        "total_items": total_items,
        "keyword_filtered_items": len(keyword_filtered),
        "ai_filtered_items": ai_filtered_count,
        "source_count": source_count,
        "output_path": str(output_path),
        "ai_enabled": ai_enabled,
    }


if __name__ == "__main__":
    result = run_catalyst_collector()
    print("\n采集完成！")
    print(f"输出文件: {result['output_path']}")