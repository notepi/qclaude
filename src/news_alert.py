"""
重要新闻预警

功能：
1. 筛选高影响级别的新闻
2. 支持按情感倾向筛选
3. 输出预警格式的新闻列表
"""

import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# 项目根目录
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data" / "catalyst" / "news"


def load_recent_news(days: int = 1) -> List[Dict]:
    """
    加载最近 N 天的新闻数据

    Args:
        days: 天数

    Returns:
        新闻列表
    """
    all_news = []
    today = datetime.now()

    for i in range(days):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y%m%d")
        file_path = DATA_DIR / f"{date_str}.json"

        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                items = data.get("items", [])
                # 添加日期标记
                for item in items:
                    item["_date"] = date_str
                all_news.extend(items)

    return all_news


def get_high_impact_news(
    sentiment: Optional[str] = None,
    impact: str = "high",
    days: int = 1
) -> List[Dict]:
    """
    获取高影响级别新闻，用于预警推送

    Args:
        sentiment: 情感倾向筛选（利好/中性/利空），None 表示不筛选
        impact: 影响级别（high/medium/low），默认 high
        days: 查询天数

    Returns:
        符合条件的新闻列表
    """
    all_news = load_recent_news(days)

    # 筛选
    filtered = []
    for news in all_news:
        news_impact = news.get("ai_impact", "unknown")
        news_sentiment = news.get("sentiment", "中性")

        # 影响级别筛选
        if impact != "all" and news_impact != impact:
            continue

        # 情感倾向筛选
        if sentiment and news_sentiment != sentiment:
            continue

        filtered.append(news)

    # 按时间排序（最新的在前）
    filtered.sort(
        key=lambda x: x.get("published_at", ""),
        reverse=True
    )

    return filtered


def format_alert_output(news_list: List[Dict], sentiment: Optional[str], impact: str) -> str:
    """
    格式化预警输出

    Args:
        news_list: 新闻列表
        sentiment: 情感倾向
        impact: 影响级别

    Returns:
        格式化的输出字符串
    """
    lines = []

    # 标题
    filter_desc = []
    if sentiment:
        filter_desc.append(f"情感: {sentiment}")
    if impact != "all":
        filter_desc.append(f"影响: {impact}")

    title = "【重要新闻预警】"
    if filter_desc:
        title += f" ({', '.join(filter_desc)})"
    lines.append(title)
    lines.append("=" * 50)

    if not news_list:
        lines.append("暂无符合条件的新闻")
        return "\n".join(lines)

    # 按股票分组
    by_stock = {}
    for news in news_list:
        stocks = news.get("related_stocks", [])
        if not stocks:
            stocks = ["[未关联股票]"]

        for stock in stocks:
            if stock not in by_stock:
                by_stock[stock] = []
            by_stock[stock].append(news)

    # 输出
    for stock, stock_news in sorted(by_stock.items()):
        lines.append(f"\n▶ {stock} ({len(stock_news)} 条)")
        lines.append("-" * 40)

        for news in stock_news:
            date = news.get("_date", "")
            if len(date) == 8:
                date = f"{date[4:6]}-{date[6:8]}"

            sentiment_mark = {"利好": "↑", "中性": "○", "利空": "↓"}.get(
                news.get("sentiment", "中性"), "○"
            )
            event_type = news.get("event_type", "事件")
            title = news.get("title", "")

            lines.append(f"  [{date}] {sentiment_mark} [{event_type}] {title}")

    lines.append(f"\n共 {len(news_list)} 条预警新闻")
    return "\n".join(lines)


def format_push_message(news: Dict) -> str:
    """
    生成单条新闻的推送消息格式

    Args:
        news: 新闻数据

    Returns:
        推送消息字符串
    """
    sentiment = news.get("sentiment", "中性")
    event_type = news.get("event_type", "事件")
    title = news.get("title", "")
    stocks = news.get("related_stocks", [])
    theme = news.get("ai_theme", "")
    reason = news.get("ai_reason", "")

    emoji_map = {"利好": "🟢", "中性": "⚪", "利空": "🔴"}
    emoji = emoji_map.get(sentiment, "⚪")

    lines = []
    lines.append(f"{emoji}【{sentiment}】【{event_type}】")
    lines.append(f"{title}")
    if stocks:
        lines.append(f"相关股票: {', '.join(stocks)}")
    if theme:
        lines.append(f"主题: {theme}")
    if reason:
        lines.append(f"理由: {reason}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="重要新闻预警")
    parser.add_argument("--sentiment", choices=["利好", "中性", "利空"], help="按情感倾向筛选")
    parser.add_argument("--impact", default="high", choices=["high", "medium", "low", "all"], help="影响级别（默认 high）")
    parser.add_argument("--days", type=int, default=1, help="查询天数（默认1天）")
    parser.add_argument("--push-format", action="store_true", help="输出推送格式")

    args = parser.parse_args()

    news_list = get_high_impact_news(
        sentiment=args.sentiment,
        impact=args.impact,
        days=args.days
    )

    if args.push_format:
        for news in news_list:
            print(format_push_message(news))
            print("-" * 30)
    else:
        output = format_alert_output(news_list, args.sentiment, args.impact)
        print(output)


if __name__ == "__main__":
    main()