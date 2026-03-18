"""
新闻催化分析系统 - 新闻采集器

功能：
1. 读取新闻源配置和筛选规则
2. 爬取新闻，提取时间戳
3. 根据关键词筛选
4. 调用 AI 进行智能筛选和标签提取
5. 保存结果
"""

import json
import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import yaml
import requests
from bs4 import BeautifulSoup

# 导入 AI 筛选模块
from ai_filter import filter_news_with_ai, check_ai_filter_available


# 项目根目录
ROOT_DIR = Path(__file__).parent.parent
CONFIG_DIR = ROOT_DIR / "config"
DATA_DIR = ROOT_DIR / "data" / "catalyst" / "news"


def load_sources() -> list[dict]:
    """加载新闻源配置"""
    config_path = CONFIG_DIR / "catalyst_sources.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return [s for s in config.get("sources", []) if s.get("enabled", True)]


def load_rules() -> dict:
    """加载筛选规则"""
    config_path = CONFIG_DIR / "catalyst_rules.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_source_type(url: str) -> str:
    """根据 URL 判断新闻源类型"""
    if "cls.cn/telegraph" in url:
        return "cls_telegraph"
    elif "10jqka.com.cn" in url:
        return "tonghuashun"
    elif "eastmoney.com" in url:
        return "eastmoney"
    else:
        return "generic"


def generate_news_id(source: str, title: str, published_at: str) -> str:
    """生成新闻唯一ID"""
    content = f"{source}_{title}_{published_at}"
    hash_val = hashlib.md5(content.encode()).hexdigest()[:8]
    date_part = datetime.now().strftime("%Y%m%d")
    return f"{source.split()[0]}_{date_part}_{hash_val}"


def parse_datetime(text: str) -> Optional[datetime]:
    """解析各种格式的时间字符串"""
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%m-%d %H:%M",  # 财联社格式：03-18 14:30
        "%H:%M",  # 只有时间
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(text.strip(), fmt)
            # 如果只有月日或时间，补充年份
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt
        except ValueError:
            continue
    return None


def fetch_cls_telegraph(url: str) -> list[dict]:
    """抓取财联社电报"""
    items = []

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = "utf-8"

        soup = BeautifulSoup(response.text, "html.parser")

        # 财联社电报页面结构：查找新闻条目
        # 尝试多种选择器
        news_elements = soup.select("div.telegraph-content, div.news-item, li.news-item, div.brief-item")

        if not news_elements:
            # 尝试更通用的选择
            news_elements = soup.find_all("div", class_=re.compile(r"(news|brief|telegraph|item)", re.I))

        for elem in news_elements:
            try:
                # 提取标题/内容
                title_elem = elem.find(["h3", "h4", "a", "span", "p"])
                if not title_elem:
                    continue

                title = title_elem.get_text(strip=True)
                if not title or len(title) < 5:
                    continue

                # 提取时间
                time_elem = elem.find(["time", "span"], class_=re.compile(r"(time|date)", re.I))
                if not time_elem:
                    time_elem = elem.find(string=re.compile(r"\d{1,2}:\d{2}|\d{2}-\d{2}"))

                published_at = None
                if time_elem:
                    time_text = time_elem.get_text(strip=True) if hasattr(time_elem, 'get_text') else str(time_elem)
                    dt = parse_datetime(time_text)
                    if dt:
                        published_at = dt

                if not published_at:
                    published_at = datetime.now()

                # 提取链接
                link_elem = elem.find("a", href=True)
                detail_url = link_elem["href"] if link_elem else ""
                if detail_url and not detail_url.startswith("http"):
                    detail_url = f"https://www.cls.cn{detail_url}"

                items.append({
                    "title": title,
                    "content": title,  # 电报通常标题即内容
                    "source": "财联社电报",
                    "source_url": url,
                    "published_at": published_at.strftime("%Y-%m-%dT%H:%M:%S"),
                    "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "url": detail_url,
                })

            except Exception as e:
                print(f"解析新闻条目出错: {e}")
                continue

        # 如果没找到任何新闻，返回提示
        if not items:
            print(f"警告: 未能从 {url} 提取到新闻，可能需要使用浏览器模式")

    except requests.RequestException as e:
        print(f"请求财联社电报失败: {e}")

    return items


def fetch_generic(url: str, source_name: str) -> list[dict]:
    """通用网页抓取"""
    items = []

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        response = requests.get(url, headers=headers, timeout=15)
        response.encoding = response.apparent_encoding or "utf-8"

        soup = BeautifulSoup(response.text, "html.parser")

        # 查找新闻列表
        for link in soup.find_all("a", href=True):
            title = link.get_text(strip=True)
            href = link["href"]

            # 过滤无效链接
            if not title or len(title) < 10:
                continue
            if href.startswith("javascript:") or href == "#":
                continue

            # 补全 URL
            if href.startswith("/"):
                parsed = urlparse(url)
                href = f"{parsed.scheme}://{parsed.netloc}{href}"

            items.append({
                "title": title,
                "content": title,
                "source": source_name,
                "source_url": url,
                "published_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "url": href,
            })

    except requests.RequestException as e:
        print(f"请求 {url} 失败: {e}")

    return items


def fetch_source(source: dict) -> list[dict]:
    """抓取单个新闻源"""
    url = source["url"]
    name = source["name"]
    source_type = get_source_type(url)

    print(f"正在抓取: {name} ({url})")

    if source_type == "cls_telegraph":
        return fetch_cls_telegraph(url)
    else:
        return fetch_generic(url, name)


def filter_news(items: list[dict], rules: dict) -> list[dict]:
    """根据规则筛选新闻"""
    include_keywords = rules.get("include_keywords", [])
    exclude_keywords = rules.get("exclude_keywords", [])

    filtered = []

    for item in items:
        title = item.get("title", "")
        content = item.get("content", "")
        text = f"{title} {content}"

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
            filtered.append(item)

    return filtered


def save_results(items: list[dict], total_items: int, source_count: int) -> Path:
    """保存结果到 JSON 文件"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    output_path = DATA_DIR / f"{today}.json"

    # 如果已有文件，合并数据
    existing_items = []
    if output_path.exists():
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            existing_items = data.get("items", [])

    # 合并并去重（按 news_id）
    all_items = {item["news_id"]: item for item in existing_items}
    for item in items:
        news_id = generate_news_id(
            item.get("source", "unknown"),
            item.get("title", ""),
            item.get("published_at", "")
        )
        item["news_id"] = news_id
        all_items[news_id] = item

    final_items = list(all_items.values())

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
    print(f"共 {len(final_items)} 条新闻（去重后）")

    return output_path


def collect_news(sources_override: list[dict] = None) -> Path:
    """
    主入口：采集新闻并保存

    Args:
        sources_override: 可选的自定义新闻源列表，用于用户临时添加 URL

    Returns:
        保存的文件路径
    """
    # 加载配置
    sources = sources_override if sources_override else load_sources()
    rules = load_rules()

    if not sources:
        print("错误: 没有配置新闻源")
        return None

    print(f"开始采集，共 {len(sources)} 个新闻源")
    print(f"筛选规则: {len(rules.get('include_keywords', []))} 个包含关键词, {len(rules.get('exclude_keywords', []))} 个排除关键词")
    print("-" * 50)

    # 抓取所有源
    all_items = []
    for source in sources:
        items = fetch_source(source)
        all_items.extend(items)
        print(f"  获取 {len(items)} 条新闻")

    print(f"\n共获取 {len(all_items)} 条新闻")

    # 关键词筛选
    filtered = filter_news(all_items, rules)
    print(f"关键词筛选后 {len(filtered)} 条新闻")

    # AI 筛选
    if check_ai_filter_available():
        filtered = filter_news_with_ai(filtered, rules)
    else:
        print("AI 筛选不可用（未配置 DASHSCOPE_API_KEY），跳过")
        # 为新闻添加默认标签
        for item in filtered:
            item.setdefault("sentiment", "中性")
            item.setdefault("related_stocks", [])
            item.setdefault("event_type", "事件")
            item.setdefault("ai_reason", "")
            item.setdefault("ai_theme", "")
            item.setdefault("ai_impact", "unknown")

    # 保存
    return save_results(filtered, len(all_items), len(sources))


def add_source(url: str, name: str = None) -> None:
    """
    添加新的新闻源到配置

    Args:
        url: 新闻源 URL
        name: 新闻源名称（可选）
    """
    if not name:
        name = url.split("//")[1].split("/")[0]  # 从 URL 提取域名

    config_path = CONFIG_DIR / "catalyst_sources.yaml"

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {"sources": []}

    # 检查是否已存在
    for source in config.get("sources", []):
        if source["url"] == url:
            print(f"新闻源已存在: {url}")
            return

    # 添加新源
    config.setdefault("sources", []).append({
        "url": url,
        "name": name,
        "enabled": True,
    })

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, allow_unicode=True, default_flow_style=False)

    print(f"已添加新闻源: {name} ({url})")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # 命令行参数：添加新闻源
        url = sys.argv[1]
        name = sys.argv[2] if len(sys.argv) > 2 else None
        add_source(url, name)
        collect_news()
    else:
        # 默认：采集所有配置的新闻源
        collect_news()