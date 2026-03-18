"""
大模型智能筛选模块

通过 OpenAI 兼容协议调用阿里百炼 Coding Plan API 进行新闻筛选。
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# API 配置
CODING_PLAN_BASE_URL = "https://coding.dashscope.aliyuncs.com/v1"
DEFAULT_MODEL = "glm-5"  # Coding Plan 支持的模型


def build_filter_prompt(news_items: List[Dict[str, Any]], rules: Dict[str, Any]) -> str:
    """
    构造新闻筛选 prompt

    Args:
        news_items: 新闻列表
        rules: 配置规则

    Returns:
        构造好的 prompt 字符串
    """
    investment_themes = rules.get("investment_themes", [])
    filter_criteria = rules.get("filter_criteria", "")

    # 格式化投资主题
    themes_text = ""
    for theme in investment_themes:
        themes_text += f"- {theme['name']}: {theme['description']}"
        if theme.get("stocks"):
            themes_text += f" (相关股票: {', '.join(theme['stocks'])})"
        themes_text += "\n"

    # 格式化新闻列表
    news_list_text = ""
    for i, item in enumerate(news_items):
        title = item.get("title", "")
        summary = item.get("summary", "")[:200] if item.get("summary") else ""
        source = item.get("source_name", "")
        news_list_text += f"[{i}] 标题: {title}\n"
        if summary:
            news_list_text += f"    摘要: {summary}\n"
        if source:
            news_list_text += f"    来源: {source}\n"
        news_list_text += "\n"

    prompt = f"""你是一个财经新闻筛选助手。根据以下投资主题和筛选标准，判断每条新闻是否应该保留。

投资主题：
{themes_text}

筛选标准：
{filter_criteria}

待筛选新闻：
{news_list_text}

请以 JSON 数组格式返回筛选结果，不要包含任何其他文字：
[
  {{
    "news_index": 0,
    "keep": true或false,
    "reason": "简短理由（不超过20字）",
    "theme": "匹配的主题名称（如保留）",
    "impact": "high或medium或low"
  }}
]

注意：
1. 只返回 JSON 数组，不要有其他文字
2. 必须为每条新闻返回一个结果
3. reason 要简短有力"""

    return prompt


def call_ai_api(prompt: str, max_retries: int = 3, retry_delay: float = 1.0, model: str = None) -> Optional[List[Dict]]:
    """
    通过 OpenAI 兼容协议调用 Coding Plan API

    Args:
        prompt: 输入 prompt
        max_retries: 最大重试次数
        retry_delay: 重试间隔（秒）
        model: 使用的模型

    Returns:
        解析后的 JSON 结果，或 None 表示失败
    """
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        print("错误: 未配置 DASHSCOPE_API_KEY 环境变量")
        return None

    model = model or os.getenv("DASHSCOPE_MODEL", DEFAULT_MODEL)
    url = f"{CODING_PLAN_BASE_URL}/chat/completions"

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 4096,
        "temperature": 0.1
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=120)

            if response.status_code == 200:
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")

                # 处理可能的 markdown 代码块
                content = content.strip()
                if content.startswith("```"):
                    lines = content.split("\n")
                    content = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])

                parsed = json.loads(content)
                return parsed
            else:
                print(f"API 调用失败: {response.status_code} - {response.text[:200]}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        except json.JSONDecodeError as e:
            print(f"JSON 解析失败: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        except requests.exceptions.Timeout:
            print("API 请求超时")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        except Exception as e:
            print(f"API 调用异常: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    return None


def filter_news_with_ai(news_items: List[Dict[str, Any]], rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    使用 AI 筛选新闻

    Args:
        news_items: 新闻列表
        rules: 配置规则

    Returns:
        筛选后的新闻列表，每条新闻增加 ai_reason、ai_theme、ai_impact 字段
    """
    ai_config = rules.get("ai_filter", {})
    enabled = ai_config.get("enabled", True)
    batch_size = ai_config.get("batch_size", 30)
    max_retries = ai_config.get("max_retries", 3)
    retry_delay = ai_config.get("retry_delay", 1.0)

    if not enabled:
        print("AI 筛选已禁用，跳过")
        return news_items

    if not news_items:
        return news_items

    print(f"开始 AI 筛选，共 {len(news_items)} 条新闻...")

    all_results = []

    # 分批处理
    for i in range(0, len(news_items), batch_size):
        batch = news_items[i:i + batch_size]
        print(f"处理第 {i//batch_size + 1} 批: {len(batch)} 条新闻")

        prompt = build_filter_prompt(batch, rules)
        results = call_ai_api(prompt, max_retries, retry_delay)

        if results:
            # 修正 news_index，加上批次偏移量
            for r in results:
                r["news_index"] = r.get("news_index", 0) + i
            all_results.extend(results)
        else:
            # API 调用失败，默认保留所有新闻
            print(f"警告: 第 {i//batch_size + 1} 批 API 调用失败，保留所有新闻")
            for j, item in enumerate(batch):
                all_results.append({
                    "news_index": i + j,  # 加上批次偏移量
                    "keep": True,
                    "reason": "API 调用失败",
                    "theme": "",
                    "impact": "unknown"
                })

        # 避免请求过快
        if i + batch_size < len(news_items):
            time.sleep(0.5)

    # 构建索引映射
    result_map = {}
    for r in all_results:
        idx = r.get("news_index")
        if idx is not None:
            result_map[idx] = r

    # 筛选并添加 AI 结果
    filtered_items = []
    for i, item in enumerate(news_items):
        result = result_map.get(i, {})
        if result.get("keep", True):
            item["ai_reason"] = result.get("reason", "")
            item["ai_theme"] = result.get("theme", "")
            item["ai_impact"] = result.get("impact", "unknown")
            filtered_items.append(item)

    print(f"AI 筛选后保留 {len(filtered_items)}/{len(news_items)} 条新闻")

    return filtered_items


def check_ai_filter_available() -> bool:
    """
    检查 AI 筛选功能是否可用

    Returns:
        True 如果可用，False 否则
    """
    api_key = os.getenv("DASHSCOPE_API_KEY")
    return bool(api_key)


if __name__ == "__main__":
    # 测试用例
    test_rules = {
        "investment_themes": [
            {
                "name": "商业航天",
                "description": "火箭发射、卫星制造、卫星互联网",
                "stocks": ["航天动力"]
            }
        ],
        "filter_criteria": "保留有投资价值的新闻"
    }

    test_news = [
        {
            "title": "航天动力获得重大订单",
            "summary": "公司宣布获得价值10亿元的火箭发动机订单",
            "source_name": "财联社"
        },
        {
            "title": "某公司澄清不涉及商业航天",
            "summary": "公司公告称不涉及商业航天业务",
            "source_name": "东方财富"
        }
    ]

    result = filter_news_with_ai(test_news, test_rules)
    print(json.dumps(result, ensure_ascii=False, indent=2))