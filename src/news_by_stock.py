"""
按股票聚合新闻

功能：
1. 查询某只股票的相关新闻
2. 支持按时间范围筛选
3. 输出格式化的新闻列表
"""

import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

# 项目根目录
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data" / "catalyst" / "news"
INDEX_DIR = ROOT_DIR / "data" / "catalyst" / "index"


def load_news_data(days: int = 7) -> List[Dict]:
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


def get_news_by_stock(stock_name: str, days: int = 7) -> List[Dict]:
    """
    获取某只股票近 N 天的相关新闻

    Args:
        stock_name: 股票名称
        days: 天数

    Returns:
        相关新闻列表
    """
    all_news = load_news_data(days)

    # 筛选包含该股票的新闻
    related_news = []
    for news in all_news:
        related_stocks = news.get("related_stocks", [])
        if stock_name in related_stocks:
            related_news.append(news)
        else:
            # 兼容旧数据：检查标题和内容
            title = news.get("title", "")
            content = news.get("content", "")
            if stock_name in title or stock_name in content:
                related_news.append(news)

    # 按时间排序（最新的在前）
    related_news.sort(
        key=lambda x: x.get("published_at", ""),
        reverse=True
    )

    return related_news


def build_stock_index() -> Dict[str, List[str]]:
    """
    构建股票索引

    Returns:
        股票名称 -> 新闻ID列表 的映射
    """
    all_news = load_news_data(30)  # 索引最近30天

    index = {}
    for news in all_news:
        news_id = news.get("news_id", "")
        date = news.get("_date", "")

        # 从 related_stocks 字段获取
        for stock in news.get("related_stocks", []):
            if stock not in index:
                index[stock] = []
            index[stock].append(f"{date}/{news_id}")

    return index


def save_stock_index(index: Dict[str, List[str]]) -> None:
    """
    保存股票索引到文件

    Args:
        index: 股票索引
    """
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    index_path = INDEX_DIR / "stocks.json"

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print(f"股票索引已保存到: {index_path}")


def format_news_output(news_list: List[Dict], stock_name: str, days: int) -> str:
    """
    格式化新闻输出

    Args:
        news_list: 新闻列表
        stock_name: 股票名称
        days: 天数

    Returns:
        格式化的输出字符串
    """
    lines = []
    lines.append(f"【{stock_name}】近 {days} 天相关新闻：")
    lines.append("-" * 50)

    if not news_list:
        lines.append("暂无相关新闻")
        return "\n".join(lines)

    for news in news_list:
        date = news.get("_date", "")
        # 格式化日期：20260318 -> 03-18
        if len(date) == 8:
            date = f"{date[4:6]}-{date[6:8]}"

        sentiment = news.get("sentiment", "中性")
        event_type = news.get("event_type", "事件")
        title = news.get("title", "")
        impact = news.get("ai_impact", "unknown")

        # 高影响加星标
        impact_mark = "★" if impact == "high" else ""

        lines.append(f"- {date} [{sentiment}][{event_type}] {impact_mark}{title}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="按股票查询相关新闻")
    parser.add_argument("stock", nargs="?", help="股票名称")
    parser.add_argument("--days", type=int, default=7, help="查询天数（默认7天）")
    parser.add_argument("--build-index", action="store_true", help="构建股票索引")
    parser.add_argument("--list-stocks", action="store_true", help="列出所有有新闻的股票")

    args = parser.parse_args()

    if args.build_index:
        index = build_stock_index()
        save_stock_index(index)
        print(f"共索引 {len(index)} 只股票")
        return

    if args.list_stocks:
        index = build_stock_index()
        print("有相关新闻的股票：")
        for stock, news_ids in sorted(index.items(), key=lambda x: -len(x[1])):
            print(f"  {stock}: {len(news_ids)} 条新闻")
        return

    if not args.stock:
        parser.print_help()
        return

    news_list = get_news_by_stock(args.stock, args.days)
    output = format_news_output(news_list, args.stock, args.days)
    print(output)


if __name__ == "__main__":
    main()