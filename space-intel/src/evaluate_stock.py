"""
股票池自动化评估模块 v1.0
基于治理规则，自动评估单只股票是否应纳入股票池以及纳入哪一层

用法：
    python evaluate_stock.py 688102.SH
    python evaluate_stock.py 688102.SH --json
    python evaluate_stock.py 688102.SH --fetch  # 强制抓取数据

设计原则：
    - 复用现有 raw/normalized 数据
    - 无数据时优雅降级，不是报错
    - 评估逻辑固化，输出标准化
"""

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml
import argparse
import json

# 确保 src/ 在 path 中
SRC_DIR = Path(__file__).parent
PROJECT_ROOT = SRC_DIR.parent
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# 常量
ANCHOR = "688333.SH"
ANCHOR_NAME = "铂力特"

# 已知股票信息库（业务相关度评估用）
# 后续可扩展为从外部 API 获取
KNOWN_STOCKS = {
    "600343.SH": {
        "name": "航天动力",
        "industry": "航天发动机/火箭推进",
        "relationship": "铂力特增材制造零件的直接下游（火箭发动机）",
        "role": "需求端参照",
    },
    "600879.SH": {
        "name": "航天电子",
        "industry": "航天电子/测控",
        "relationship": "同属航天制造体系，情绪联动有效",
        "role": "交易层参照",
    },
    "600118.SH": {
        "name": "中国卫星",
        "industry": "卫星研制/卫星应用",
        "relationship": "卫星结构件是铂力特增材制造应用场景",
        "role": "交易层参照",
    },
    "688433.SH": {
        "name": "华曙高科",
        "industry": "增材制造设备",
        "relationship": "同属增材制造赛道，设备端 vs 零件端互补",
        "role": "同赛道估值对标",
    },
    "003009.SZ": {
        "name": "中天火箭",
        "industry": "固体火箭/商业航天",
        "relationship": "火箭结构件/发动机零件的下游客户类型",
        "role": "需求端验证",
    },
    "688102.SH": {
        "name": "斯瑞新材",
        "industry": "高性能铜合金/难熔金属材料",
        "relationship": "金属粉末原材料供应商，材料端 vs 制造端",
        "role": "材料层参照",
    },
    "601698.SH": {
        "name": "中国卫通",
        "industry": "卫星通信运营",
        "relationship": "卫星通信运营商，与铂力特同受低轨卫星政策催化",
        "role": "交易层候选（待观察）",
    },
    "300762.SZ": {
        "name": "上海瀚讯",
        "industry": "卫星通信终端",
        "relationship": "卫星通信应用端，产业链位置与应用层",
        "role": "extended 观察",
    },
    "600151.SH": {
        "name": "航天工程",
        "industry": "煤化工工程",
        "relationship": "主业为煤化工，与商业航天无实质关联",
        "role": "降级观察（已移除核心层）",
    },
}


def get_project_paths():
    """获取项目路径"""
    return {
        "raw": PROJECT_ROOT / "data" / "raw" / "market_data.parquet",
        "normalized": PROJECT_ROOT / "data" / "normalized" / "market_data_normalized.parquet",
        "config": PROJECT_ROOT / "config" / "stocks.yaml",
    }


def load_stock_data():
    """加载现有市场数据"""
    paths = get_project_paths()
    
    # 优先从 normalized 读取
    if paths["normalized"].exists():
        df = pd.read_parquet(paths["normalized"])
        source = "normalized"
    elif paths["raw"].exists():
        df = pd.read_parquet(paths["raw"])
        source = "raw"
    else:
        return None, "no_data"
    
    # 确保日期格式
    if "trade_date" in df.columns:
        if df["trade_date"].dtype.kind != "M":
            df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
    
    return df, source


def load_stock_config():
    """加载股票池配置"""
    paths = get_project_paths()
    if not paths["config"].exists():
        return None
    
    with open(paths["config"], encoding="utf-8") as f:
        return yaml.safe_load(f)


def compute_correlations(symbol: str, df: pd.DataFrame) -> dict:
    """
    计算与铂力特的相关系数
    
    Returns:
        dict: {
            "full_corr": float,      # 全量相关系数
            "corr_20d": float,       # 近20日相关系数
            "corr_10d": float,       # 近10日相关系数
            "ret_20d": float,        # 近20日涨跌幅
            "data_points": int,      # 数据点数
            "date_range": str,       # 数据日期范围
        }
    """
    if df is None or df.empty:
        return {"error": "no_data"}
    
    # 过滤目标股票
    if symbol not in df["ts_code"].values:
        return {"error": "symbol_not_in_data"}
    
    if ANCHOR not in df["ts_code"].values:
        return {"error": "anchor_not_in_data"}
    
    # 构建 pivot 表
    pivot = df.pivot_table(
        index="trade_date", 
        columns="ts_code", 
        values="close"
    ).sort_index()
    
    # 计算日收益率
    returns = pivot.pct_change().dropna()
    
    if symbol not in returns.columns or ANCHOR not in returns.columns:
        return {"error": "insufficient_data"}
    
    # 全量相关系数
    full_corr = returns[ANCHOR].corr(returns[symbol])
    
    # 近20日
    r20 = returns.tail(20)
    corr_20d = r20[ANCHOR].corr(r20[symbol]) if len(r20) >= 10 else None
    
    # 近10日
    r10 = returns.tail(10)
    corr_10d = r10[ANCHOR].corr(r10[symbol]) if len(r10) >= 5 else None
    
    # 近20日涨跌幅
    if len(pivot) >= 21:
        ret_20d = (pivot[symbol].iloc[-1] / pivot[symbol].iloc[-21] - 1) if pivot[symbol].iloc[-21] > 0 else None
    else:
        ret_20d = None
    
    # 日期范围
    dates = sorted(df[df["ts_code"] == symbol]["trade_date"].unique())
    date_range = f"{pd.Timestamp(dates[0]).strftime('%Y%m%d')}~{pd.Timestamp(dates[-1]).strftime('%Y%m%d')}" if dates else "N/A"
    
    return {
        "full_corr": round(full_corr, 3) if full_corr is not None else None,
        "corr_20d": round(corr_20d, 3) if corr_20d is not None else None,
        "corr_10d": round(corr_10d, 3) if corr_10d is not None else None,
        "ret_20d": round(ret_20d, 4) if ret_20d is not None else None,
        "data_points": len(dates),
        "date_range": date_range,
    }


def get_stock_info(symbol: str) -> dict:
    """获取股票基本信息"""
    # 优先从已知库获取
    if symbol in KNOWN_STOCKS:
        return KNOWN_STOCKS[symbol]
    
    # 尝试从配置中获取
    config = load_stock_config()
    if config:
        for layer in ["core_universe", "research_core", "extended_universe", 
                      "research_candidates", "trading_candidates"]:
            for item in config.get(layer, []):
                if item.get("code") == symbol:
                    return {
                        "name": item.get("name", "未知"),
                        "industry": item.get("tags", ["未知"])[0] if item.get("tags") else "未知",
                        "relationship": item.get("reason", "待评估"),
                        "role": item.get("layer", "待评估"),
                    }
    
    return {
        "name": "未知",
        "industry": "未知",
        "relationship": "待评估",
        "role": "新候选",
    }


def assess_pollution_risk(correlations: dict, business: dict) -> dict:
    """
    评估污染风险
    
    基于：
    1. 相关系数是否足够稳定
    2. 业务逻辑是否与商业航天主线相关
    3. 是否可能引入非相关噪音
    """
    if "error" in correlations:
        return {"risk_level": "unknown", "reasons": ["数据不足，无法评估"]}
    
    risk_factors = []
    risk_score = 0  # 0-10, 越高风险越大
    
    # 相关系数检查
    c20 = correlations.get("corr_20d")
    c10 = correlations.get("corr_10d")
    
    if c20 is not None and c20 < 0.50:
        risk_factors.append(f"近20日相关系数偏低 ({c20:.2f})")
        risk_score += 3
    elif c20 is not None and c20 < 0.60:
        risk_factors.append(f"近20日相关系数一般 ({c20:.2f})")
        risk_score += 1
    
    if c10 is not None and c10 < 0.40:
        risk_factors.append(f"近10日相关系数过低 ({c10:.2f})，可能已失联")
        risk_score += 4
    
    # 相关系数下降检查
    if c20 is not None and c10 is not None:
        if c10 < c20 - 0.20:
            risk_factors.append(f"相关系数近期明显下滑 ({c20:.2f} → {c10:.2f})")
            risk_score += 2
    
    # 业务逻辑检查
    industry = business.get("industry", "").lower()
    relation = business.get("relationship", "").lower()
    
    problematic_keywords = ["煤化工", "房地产", "金融", "传统制造"]
    for kw in problematic_keywords:
        if kw in industry or kw in relation:
            risk_factors.append(f"主营业务可能与商业航天无关 ({industry})")
            risk_score += 5
    
    # 风险等级
    if risk_score >= 6:
        risk_level = "high"
    elif risk_score >= 3:
        risk_level = "medium"
    else:
        risk_level = "low"
    
    return {
        "risk_level": risk_level,
        "risk_score": risk_score,
        "factors": risk_factors if risk_factors else ["未发现明显风险"],
    }


def evaluate_layer_eligibility(symbol: str, correlations: dict, business: dict, pollution: dict) -> dict:
    """
    综合评估，输出分层建议
    
    准入规则（参考 pool_governance.md）：
    - trading_core: 必须是"直接制造/发射主线"，相关性+业务双重约束
    - research_core: 产业链上下游/同赛道/研究价值
    - trading_candidate: 高相关但非核心主线
    - extended: 观察，不纳入核心层
    
    硬约束：
    - 材料端/军工电子/通信应用/软件数据 → 最高 trading_candidate
    - 污染风险 high → 最高 trading_candidate
    """
    result = {
        "recommended_layer": None,
        "priority": None,
        "conditions": [],
        "reason": "",
    }
    
    # 数据不足
    if "error" in correlations:
        result["recommended_layer"] = "unknown"
        result["reason"] = "数据不足，无法评估"
        return result
    
    c20 = correlations.get("corr_20d")
    c10 = correlations.get("corr_10d")
    full_corr = correlations.get("full_corr")
    
    # 检查1: 是否在配置中已有分层
    config = load_stock_config()
    if config:
        for layer_name, layer_key in [
            ("trading_core", "core_universe"),
            ("research_core", "research_core"),
            ("extended", "extended_universe"),
        ]:
            for item in config.get(layer_key, []):
                if item.get("code") == symbol and item.get("benchmark_included"):
                    result["recommended_layer"] = layer_name
                    result["reason"] = f"已在配置中 (layer={layer_name})"
                    result["current_status"] = "existing"
                    return result
    
    relationship = business.get("relationship", "").lower()
    industry = business.get("industry", "").lower()
    
    # ===== 硬约束检查（优先级最高）=====
    
    # 约束1: 主业与商业航天无关 → 直接 extended
    if any(kw in industry for kw in ["煤化工", "房地产", "金融", "传统制造"]):
        result["recommended_layer"] = "extended_watchlist"
        result["reason"] = f"主业与商业航天无关 ({industry})"
        result["conditions"].append("强制降级：主业不符")
        return result
    
    # 约束2: 污染风险 high → 最高 trading_candidate
    if pollution.get("risk_level") == "high":
        result["recommended_layer"] = "trading_candidates"
        result["priority"] = 3
        result["reason"] = "污染风险高，限制进入核心层"
        result["conditions"].append(f"污染风险: {pollution.get('risk_level')}")
        return result
    
    # 约束3: 非核心业务属性 → 最高 trading_candidate
    # 这些类型即使相关性高，也不能进 trading_core
    non_core_types = {
        "material": ["材料", "粉末", "合金", "铜合金", "难熔金属", "高温合金", "金属材料"],
        "military_electronic": ["电子", "电容", "电阻", "连接器", "军工", "mlcc", "元器件"],
        "telecom_application": ["通信终端", "终端", "应用端", "通信运营", "地面设备", "宽带"],
        "software_data": ["软件", "gis", "遥感", "数据服务", "卫星数据"],
    }
    
    matched_non_core = []
    for category, keywords in non_core_types.items():
        for kw in keywords:
            if kw in relationship or kw in industry:
                matched_non_core.append(category)
                break
    
    # ===== 核心主线判定 =====
    # 只有这些才算"直接制造/发射主线"
    core_mainline_keywords = [
        "增材制造", "3d打印", "零件", "发动机", "火箭", 
        "发射", "卫星制造", "结构件", "航天动力", "航天电子"
    ]
    is_core_mainline = any(kw in relationship for kw in core_mainline_keywords)
    
    trading_conditions = []
    max_layer = "trading_core"  # 默认可进入 trading_core
    
    # 如果是非核心类型，降低上限
    if matched_non_core:
        max_layer = "trading_candidates"
        trading_conditions.append(f"非核心业务属性: {', '.join(set(matched_non_core))}")
    
    # ===== 相关性检查 =====
    if c20 is not None and c20 >= 0.60:
        trading_conditions.append(f"近20日相关系数 {c20:.2f} ✓")
    else:
        trading_conditions.append(f"近20日相关系数需≥0.60 (当前: {c20})")
    
    if c10 is not None and c10 >= 0.50:
        trading_conditions.append(f"近10日相关系数 {c10:.2f} ✓")
    else:
        trading_conditions.append(f"近10日相关系数需≥0.50 (当前: {c10})")
    
    # ===== 最终分层决策 =====
    
    # 必须同时满足核心主线 + 相关性门槛 才能进 trading_core
    trading_core_ready = (
        is_core_mainline and 
        (c20 is None or c20 >= 0.60) and 
        (c10 is None or c10 >= 0.50) and
        max_layer == "trading_core"
    )
    
    if trading_core_ready:
        result["recommended_layer"] = "trading_core"
        result["priority"] = 1
        result["reason"] = "满足 trading_core 准入条件：核心主线 + 高相关性"
        result["conditions"] = trading_conditions
        return result
    
    # research_core 准入：业务逻辑相关
    if is_core_mainline or any(kw in relationship for kw in ["增材制造", "3d打印", "设备", "零件", "发动机", "火箭", "材料", "粉末"]):
        result["recommended_layer"] = "research_core"
        result["priority"] = 2
        result["reason"] = "业务逻辑与铂力特相关，可作为研究对标"
        result["conditions"] = trading_conditions
        return result
    
    # trading_candidate 准入：高相关性但非核心主线
    if full_corr is not None and full_corr > 0.50:
        result["recommended_layer"] = "trading_candidates"
        result["priority"] = 3
        result["reason"] = f"高相关性（{full_corr:.2f}），但非核心主线，候选观察"
        result["conditions"] = trading_conditions
        return result
    
    # 默认: extended
    result["recommended_layer"] = "extended_watchlist"
    result["priority"] = 4
    result["reason"] = "不满足核心层准入条件"
    result["conditions"] = trading_conditions
    return result


def generate_evaluation_report(symbol: str, format: str = "text") -> str:
    """
    生成评估报告
    
    Args:
        symbol: 股票代码
        format: 输出格式 ("text" 或 "json")
    
    Returns:
        str: 报告内容
    """
    # 1. 加载数据
    df, source = load_stock_data()
    
    # 2. 获取股票信息
    business = get_stock_info(symbol)
    
    # 3. 计算相关系数
    correlations = compute_correlations(symbol, df) if df is not None else {"error": "no_data"}
    
    # 4. 评估污染风险
    pollution = assess_pollution_risk(correlations, business)
    
    # 5. 综合评估
    eligibility = evaluate_layer_eligibility(symbol, correlations, business, pollution)
    
    # 构建结果
    result = {
        "symbol": symbol,
        "name": business.get("name", "未知"),
        "industry": business.get("industry", "未知"),
        "relationship": business.get("relationship", "待评估"),
        "data_status": {
            "available": "error" not in correlations,
            "source": source,
            "data_points": correlations.get("data_points", 0),
            "date_range": correlations.get("date_range", "N/A"),
        },
        "correlations": correlations,
        "pollution_risk": pollution,
        "eligibility": eligibility,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    
    if format == "json":
        return json.dumps(result, ensure_ascii=False, indent=2)
    
    # 文本格式
    lines = [
        f"{'='*60}",
        f"股票池评估报告 - {symbol} {business.get('name', '')}",
        f"{'='*60}",
        "",
        f"【基本信息】",
        f"  行业: {business.get('industry', '未知')}",
        f"  与铂力特关系: {business.get('relationship', '待评估')}",
        "",
        f"【数据状态】",
        f"  数据可用: {'是' if result['data_status']['available'] else '否'}",
        f"  数据来源: {source}",
        f"  数据点数: {result['data_status']['data_points']}",
        f"  日期范围: {result['data_status']['date_range']}",
        "",
        f"【交易联动性】",
    ]
    
    if "error" in correlations:
        lines.append(f"  状态: 无法计算（{correlations.get('error', '未知错误')}）")
    else:
        lines.extend([
            f"  全量相关系数: {correlations.get('full_corr', 'N/A')}",
            f"  近20日相关系数: {correlations.get('corr_20d', 'N/A')}",
            f"  近10日相关系数: {correlations.get('corr_10d', 'N/A')}",
            f"  近20日涨跌幅: {correlations.get('ret_20d', 'N/A'):.2%}" if correlations.get("ret_20d") else "  近20日涨跌幅: N/A",
        ])
    
    lines.extend([
        "",
        f"【污染风险评估】",
        f"  风险等级: {pollution.get('risk_level', 'unknown').upper()}",
        f"  风险因素: {'; '.join(pollution.get('factors', ['无'])[:3])}",
        "",
        f"【分层建议】",
        f"  推荐层级: {eligibility.get('recommended_layer', 'unknown')}",
        f"  优先级: {eligibility.get('priority', 'N/A')}",
        f"  原因: {eligibility.get('reason', 'N/A')}",
    ])
    
    if eligibility.get("conditions"):
        lines.append(f"  条件: {'; '.join(eligibility['conditions'][:3])}")
    
    lines.extend([
        "",
        f"【配置状态】",
        f"  当前层级: {eligibility.get('current_status', '新候选')}",
        "",
        f"{'='*60}",
        f"生成时间: {result['generated_at']}",
    ])
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="股票池自动化评估工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  python evaluate_stock.py 688102.SH
  python evaluate_stock.py 688102.SH --json
  python evaluate_stock.py 688102.SH --fetch  # 强制抓取数据
        """,
    )
    parser.add_argument("symbol", help="股票代码（如 688102.SH）")
    parser.add_argument("--json", action="store_true", help="JSON 格式输出")
    parser.add_argument("--fetch", action="store_true", help="强制抓取数据（如本地无）")
    
    args = parser.parse_args()
    
    # 处理股票代码格式
    symbol = args.symbol.upper().strip()
    if "." not in symbol:
        # 尝试自动补齐 .SH/.SZ
        if symbol.startswith("688"):
            symbol += ".SH"
        elif symbol.startswith("6"):
            symbol += ".SH"
        elif symbol.startswith("0") or symbol.startswith("3"):
            symbol += ".SZ"
    
    # 评估
    report = generate_evaluation_report(symbol, format="json" if args.json else "text")
    print(report)


if __name__ == "__main__":
    main()
