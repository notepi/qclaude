"""
股票池复审提醒模块 v1.0
输出哪些标的需要复审、为什么、建议动作

用法：
    python3 src/review_stock_pool.py
    python3 src/review_stock_pool.py --output-json

规则：
    - review_date < today -> due -> review
    - 0 <= days_to_review <= 7 -> upcoming -> review
    - days_to_review > 7 -> normal -> maintain
    - active=false -> inactive -> maintain
"""

import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import yaml

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "stocks.yaml"


def load_stock_config() -> dict:
    """加载股票池配置"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def parse_review_date(date_str) -> datetime:
    """解析 review_date"""
    if date_str is None:
        return None
    if isinstance(date_str, datetime):
        return date_str
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%d")
    except:
        return None


def calc_review_status(review_date: datetime, active: bool, today: datetime = None) -> dict:
    """
    计算复审状态
    
    Returns:
        {
            "review_status": str,  # due / upcoming / normal / inactive
            "days_to_review": int,  # 负数表示已过期
            "reason_flags": list,
            "review_suggestion": str,
        }
    """
    if today is None:
        today = datetime.now()
    
    # inactive 状态
    if not active:
        return {
            "review_status": "inactive",
            "days_to_review": None,
            "reason_flags": ["当前未激活"],
            "review_suggestion": "maintain",
        }
    
    # 无 review_date
    if review_date is None:
        return {
            "review_status": "normal",
            "days_to_review": None,
            "reason_flags": ["未设置复审日期"],
            "review_suggestion": "maintain",
        }
    
    days_to_review = (review_date - today).days
    
    # due: 已过期
    if days_to_review < 0:
        return {
            "review_status": "due",
            "days_to_review": days_to_review,
            "reason_flags": [f"复审已过期 {abs(days_to_review)} 天"],
            "review_suggestion": "review",
        }
    
    # upcoming: 7天内到期
    if days_to_review <= 7:
        return {
            "review_status": "upcoming",
            "days_to_review": days_to_review,
            "reason_flags": [f"复审即将到期 ({days_to_review} 天)"],
            "review_suggestion": "review",
        }
    
    # normal
    return {
        "review_status": "normal",
        "days_to_review": days_to_review,
        "reason_flags": [],
        "review_suggestion": "maintain",
    }


def analyze_pool(config: dict, today: datetime = None) -> list:
    """
    分析整个股票池
    
    Returns:
        list of dict, 每个标的的分析结果
    """
    if today is None:
        today = datetime.now()
    
    results = []
    
    # 遍历所有层级
    layers = [
        ("anchor", [config.get("anchor", {})]),
        ("trading_core", config.get("core_universe", [])),
        ("research_core", config.get("research_core", [])),
        ("trading_candidates", config.get("trading_candidates", [])),
        ("research_candidates", config.get("research_candidates", [])),
        ("extended_watchlist", config.get("extended_universe", [])),
    ]
    
    for layer_name, stocks in layers:
        for stock in stocks:
            if not stock:
                continue
            
            code = stock.get("code", "")
            name = stock.get("name", "")
            active = stock.get("active", True)
            review_date = parse_review_date(stock.get("review_date"))
            reason = stock.get("reason", "")
            
            status = calc_review_status(review_date, active, today)
            
            results.append({
                "ts_code": code,
                "name": name,
                "pool_role": layer_name,
                "active": active,
                "review_date": review_date.strftime("%Y-%m-%d") if review_date else None,
                **status,
                "original_reason": reason,
            })
    
    return results


def generate_summary(results: list) -> dict:
    """生成汇总统计"""
    due = [r for r in results if r["review_status"] == "due"]
    upcoming = [r for r in results if r["review_status"] == "upcoming"]
    inactive = [r for r in results if r["review_status"] == "inactive"]
    
    return {
        "total_stocks": len(results),
        "due_count": len(due),
        "upcoming_count": len(upcoming),
        "inactive_count": len(inactive),
        "due_list": due,
        "upcoming_list": upcoming,
        "inactive_list": inactive,
    }


def format_output(results: list, summary: dict) -> str:
    """格式化输出"""
    lines = [
        "=" * 60,
        "股票池复审提醒 v1.0",
        "=" * 60,
        f"\n汇总: 共 {summary['total_stocks']} 只标的",
        f"  - 需立即复审: {summary['due_count']} 只",
        f"  - 即将到期: {summary['upcoming_count']} 只",
        f"  - 未激活: {summary['inactive_count']} 只",
    ]
    
    # due 列表
    if summary["due_list"]:
        lines.append("\n" + "-" * 60)
        lines.append("【需立即复审】")
        for item in summary["due_list"]:
            lines.append(f"\n  {item['name']} ({item['ts_code']})")
            lines.append(f"    层级: {item['pool_role']}")
            lines.append(f"    复审日: {item['review_date']} (已过期 {abs(item['days_to_review'])} 天)")
            lines.append(f"    建议: {item['review_suggestion']}")
            lines.append(f"    原因: {'; '.join(item['reason_flags'])}")
    
    # upcoming 列表
    if summary["upcoming_list"]:
        lines.append("\n" + "-" * 60)
        lines.append("【即将到期】")
        for item in summary["upcoming_list"]:
            lines.append(f"\n  {item['name']} ({item['ts_code']})")
            lines.append(f"    层级: {item['pool_role']}")
            lines.append(f"    复审日: {item['review_date']} ({item['days_to_review']} 天后)")
            lines.append(f"    建议: {item['review_suggestion']}")
    
    lines.append("\n" + "=" * 60)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="股票池复审提醒 v1.0")
    parser.add_argument("--output-json", action="store_true", help="JSON 格式输出")
    parser.add_argument("--date", type=str, help="指定日期 (YYYY-MM-DD)")
    args = parser.parse_args()
    
    today = datetime.now()
    if args.date:
        today = datetime.strptime(args.date, "%Y-%m-%d")
    
    # 加载配置
    config = load_stock_config()
    
    # 分析
    results = analyze_pool(config, today)
    summary = generate_summary(results)
    
    # 输出
    if args.output_json:
        import json
        output = {
            "review_due_list": summary["due_list"],
            "review_upcoming_list": summary["upcoming_list"],
            "review_summary": {
                "total": summary["total_stocks"],
                "due": summary["due_count"],
                "upcoming": summary["upcoming_count"],
                "inactive": summary["inactive_count"],
            },
            "generated_at": today.strftime("%Y-%m-%d %H:%M:%S"),
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
    else:
        print(format_output(results, summary))


if __name__ == "__main__":
    main()
