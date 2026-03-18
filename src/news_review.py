"""
复盘验证脚本

功能：
1. 查看某天的新闻与后续股价表现对比
2. 验证新闻催化效果
3. 生成复盘报告
"""

import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# 项目根目录
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data" / "catalyst" / "news"


def load_news_by_date(date: str) -> List[Dict]:
    """
    加载指定日期的新闻

    Args:
        date: 日期字符串（YYYYMMDD 或 YYYY-MM-DD）

    Returns:
        新闻列表
    """
    # 标准化日期格式
    date = date.replace("-", "")

    file_path = DATA_DIR / f"{date}.json"
    if not file_path.exists():
        return []

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        items = data.get("items", [])
        for item in items:
            item["_date"] = date
        return items


def review_news_performance(date: str) -> Dict:
    """
    查看某天的新闻与后续股价表现对比

    Args:
        date: 日期字符串

    Returns:
        复盘结果字典
    """
    news_list = load_news_by_date(date)

    result = {
        "date": date,
        "total_news": len(news_list),
        "by_stock": {},
        "by_sentiment": {
            "利好": 0,
            "中性": 0,
            "利空": 0
        },
        "by_impact": {
            "high": 0,
            "medium": 0,
            "low": 0,
            "unknown": 0
        },
        "by_event_type": {},
        "high_impact_news": []
    }

    for news in news_list:
        # 按股票分组
        stocks = news.get("related_stocks", [])
        if not stocks:
            stocks = ["[未关联]"]

        for stock in stocks:
            if stock not in result["by_stock"]:
                result["by_stock"][stock] = []
            result["by_stock"][stock].append({
                "title": news.get("title", ""),
                "sentiment": news.get("sentiment", "中性"),
                "impact": news.get("ai_impact", "unknown"),
                "event_type": news.get("event_type", "事件"),
                "theme": news.get("ai_theme", "")
            })

        # 按情感统计
        sentiment = news.get("sentiment", "中性")
        if sentiment in result["by_sentiment"]:
            result["by_sentiment"][sentiment] += 1

        # 按影响级别统计
        impact = news.get("ai_impact", "unknown")
        if impact in result["by_impact"]:
            result["by_impact"][impact] += 1

        # 按事件类型统计
        event_type = news.get("event_type", "事件")
        result["by_event_type"][event_type] = result["by_event_type"].get(event_type, 0) + 1

        # 收集高影响新闻
        if impact == "high":
            result["high_impact_news"].append({
                "title": news.get("title", ""),
                "stocks": stocks,
                "sentiment": sentiment,
                "event_type": event_type
            })

    return result


def format_review_output(review: Dict) -> str:
    """
    格式化复盘输出

    Args:
        review: 复盘结果

    Returns:
        格式化的输出字符串
    """
    lines = []

    date = review["date"]
    # 格式化日期
    if len(date) == 8:
        date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"

    lines.append(f"【新闻复盘】{date}")
    lines.append("=" * 50)
    lines.append(f"总新闻数: {review['total_news']} 条")
    lines.append("")

    # 情感分布
    lines.append("【情感分布】")
    sentiment = review["by_sentiment"]
    total = sum(sentiment.values())
    if total > 0:
        for s, count in sentiment.items():
            pct = count / total * 100 if total > 0 else 0
            bar = "█" * int(pct / 5)
            lines.append(f"  {s}: {count:3d} ({pct:5.1f}%) {bar}")
    lines.append("")

    # 影响级别分布
    lines.append("【影响级别】")
    impact = review["by_impact"]
    for i, count in impact.items():
        if count > 0:
            lines.append(f"  {i}: {count} 条")
    lines.append("")

    # 事件类型分布
    lines.append("【事件类型】")
    for etype, count in sorted(review["by_event_type"].items(), key=lambda x: -x[1]):
        lines.append(f"  {etype}: {count} 条")
    lines.append("")

    # 按股票汇总
    lines.append("【按股票汇总】")
    for stock, news_items in sorted(review["by_stock"].items(), key=lambda x: -len(x[1])):
        lines.append(f"\n  ▶ {stock} ({len(news_items)} 条)")
        for n in news_items[:5]:  # 只显示前5条
            sentiment_mark = {"利好": "↑", "中性": "○", "利空": "↓"}.get(n["sentiment"], "○")
            impact_mark = "★" if n["impact"] == "high" else ""
            lines.append(f"    {sentiment_mark} [{n['event_type']}] {impact_mark}{n['title'][:30]}...")
        if len(news_items) > 5:
            lines.append(f"    ... 还有 {len(news_items) - 5} 条")
    lines.append("")

    # 高影响新闻
    if review["high_impact_news"]:
        lines.append("【高影响新闻】")
        for n in review["high_impact_news"]:
            stocks_str = ", ".join(n["stocks"]) if n["stocks"] else "[未关联]"
            sentiment_mark = {"利好": "↑", "中性": "○", "利空": "↓"}.get(n["sentiment"], "○")
            lines.append(f"  {sentiment_mark} [{n['event_type']}] {n['title']}")
            lines.append(f"      相关: {stocks_str}")

    return "\n".join(lines)


def compare_periods(date1: str, date2: str) -> str:
    """
    对比两个日期的新闻情况

    Args:
        date1: 第一个日期
        date2: 第二个日期

    Returns:
        对比结果字符串
    """
    review1 = review_news_performance(date1)
    review2 = review_news_performance(date2)

    lines = []
    lines.append("【新闻对比分析】")
    lines.append("=" * 50)
    lines.append(f"日期: {date1} vs {date2}")
    lines.append("")

    # 新闻数量对比
    lines.append("【新闻数量】")
    lines.append(f"  {date1}: {review1['total_news']} 条")
    lines.append(f"  {date2}: {review2['total_news']} 条")
    diff = review2['total_news'] - review1['total_news']
    change = f"+{diff}" if diff > 0 else str(diff)
    lines.append(f"  变化: {change}")
    lines.append("")

    # 情感对比
    lines.append("【情感对比】")
    for sentiment in ["利好", "中性", "利空"]:
        c1 = review1["by_sentiment"].get(sentiment, 0)
        c2 = review2["by_sentiment"].get(sentiment, 0)
        lines.append(f"  {sentiment}: {c1} -> {c2}")
    lines.append("")

    # 高影响新闻对比
    lines.append("【高影响新闻】")
    lines.append(f"  {date1}: {len(review1['high_impact_news'])} 条")
    lines.append(f"  {date2}: {len(review2['high_impact_news'])} 条")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="新闻复盘验证")
    parser.add_argument("date", nargs="?", help="复盘日期（YYYYMMDD 或 YYYY-MM-DD）")
    parser.add_argument("--compare", metavar="DATE2", help="对比另一个日期")
    parser.add_argument("--recent", type=int, help="查看最近 N 天的复盘汇总")

    args = parser.parse_args()

    if args.recent:
        # 最近 N 天汇总
        lines = []
        lines.append("【近期新闻汇总】")
        lines.append("=" * 50)

        today = datetime.now()
        total_high_impact = 0
        all_stocks = {}

        for i in range(args.recent):
            date = today - timedelta(days=i)
            date_str = date.strftime("%Y%m%d")
            review = review_news_performance(date_str)

            if review["total_news"] > 0:
                lines.append(f"\n{date_str}: {review['total_news']} 条新闻")
                lines.append(f"  利好: {review['by_sentiment']['利好']}, "
                           f"中性: {review['by_sentiment']['中性']}, "
                           f"利空: {review['by_sentiment']['利空']}")
                total_high_impact += len(review["high_impact_news"])

                for stock in review["by_stock"]:
                    all_stocks[stock] = all_stocks.get(stock, 0) + len(review["by_stock"][stock])

        lines.append(f"\n【汇总统计】")
        lines.append(f"总高影响新闻: {total_high_impact} 条")
        lines.append(f"涉及股票: {len(all_stocks)} 只")

        # 显示新闻最多的股票
        top_stocks = sorted(all_stocks.items(), key=lambda x: -x[1])[:5]
        lines.append("\n新闻最多的股票:")
        for stock, count in top_stocks:
            lines.append(f"  {stock}: {count} 条")

        print("\n".join(lines))
        return

    if not args.date:
        # 默认今天
        args.date = datetime.now().strftime("%Y%m%d")

    if args.compare:
        output = compare_periods(args.date, args.compare)
        print(output)
    else:
        review = review_news_performance(args.date)
        output = format_review_output(review)
        print(output)


if __name__ == "__main__":
    main()