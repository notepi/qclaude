#!/usr/bin/env python3
"""
铂力特每日复盘日报生成器
用法: python3 daily_report.py [日期YYYY-MM-DD]
     默认生成最近交易日
"""

import pandas as pd
import sys
import os
from datetime import datetime, timedelta

# 配置路径
DATA_PATH = '/Users/pan/Desktop/qclaude/space-intel/data/analytics/scored_metrics.parquet'
OUTPUT_DIR = '/Users/pan/Desktop/qclaude/space-intel/daily-reports'

def load_data():
    """加载数据"""
    df = pd.read_parquet(DATA_PATH)
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
    return df

def get_recent_trading_days(df, n=5):
    """获取最近n个交易日"""
    return df.tail(n).copy()

def generate_report_content(row, prev_rows=None):
    """生成单日报表内容"""
    trade_date = row['trade_date']
    
    # 状态面板
    report = f"""---
date: {trade_date}
generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
---

# 铂力特短周期交易研究日报

## 1. 状态面板

| 指标 | 数值/状态 |
|------|-----------|
| 交易日 | {trade_date} |
| 整体信号 | {row['overall_signal_label']} |
| 价格强度 | {row['price_strength_label']} |
| 量能强度 | {row['volume_strength_label']} |
| 锚定收益 | {row['anchor_return']*100:.2f}% |
| 信号评分 | {row['signal_score']:.2f} |

---

## 2. 今日结论

"""
    
    # 今日结论
    signal = row['overall_signal_label']
    price_strength = row['price_strength_label']
    anchor_return = row['anchor_return']
    relative_strength = row['relative_strength']
    capital_flow = row['capital_flow_label']
    price_capital = row['price_capital_relation_label']
    
    # 当前状态
    if '偏强' in signal:
        current_state = f"整体信号维持{signal}，价格强度保持'{price_strength}'档位。"
    elif '偏弱' in signal:
        current_state = f"整体信号转为{signal}，价格强度走弱，锚定收益明显负向。"
    else:
        current_state = f"整体信号为{signal}。"
    
    # 主要原因
    reasons = []
    if price_strength == '强':
        reasons.append(f"价格强度保持'{price_strength}'，相对强度达{relative_strength*100:.2f}%")
    elif price_strength == '弱':
        reasons.append(f"价格强度由前一日的'强'转为'弱'，相对强度转负")
    
    if capital_flow == '主力中性':
        reasons.append("资金流从'偏空'转为'主力中性'")
    elif capital_flow == '主力偏空':
        reasons.append("主力资金呈偏空态度")
    
    if price_capital == '下行配合':
        reasons.append("价格与资金关系呈现'下行配合'特征")
    elif price_capital == '中性':
        reasons.append("价格-资金关系转为'中性'")
    
    main_reasons = '；'.join(reasons) + '。'
    
    # 下一步观察
    if '偏强' in signal:
        next_obs = "关注价格强度能否持续，以及资金流能否进一步转好。"
    elif '偏弱' in signal:
        next_obs = "关注后续交易日信号能否企稳，若价格强度持续走弱，需警惕短期回撤风险。"
    else:
        next_obs = "关注信号变化方向。"
    
    report += f"""**当前状态：** {current_state}

**主要原因：** {main_reasons}

**下一步观察：** {next_obs}

---

## 3. 近5日状态

"""
    
    # 近5日状态分析
    if prev_rows is not None and len(prev_rows) >= 5:
        recent5 = prev_rows.tail(5)
        strong_days = recent5[recent5['overall_signal_label'].str.contains('偏强')].shape[0]
        weak_days = recent5[recent5['overall_signal_label'].str.contains('偏弱')].shape[0]
        
        # 检查资金流变化
        capital_flows = recent5['capital_flow_label'].tolist()
        latest_capital = capital_flows[-1]
        
        # 检查量能
        volume_strengths = recent5['volume_strength_label'].tolist()
        
        if strong_days >= 3:
            structure_status = f"近5日结构尚未完全破坏，共{strong_days}日维持'中性偏强'信号。"
        elif weak_days >= 3:
            structure_status = f"近5日结构偏弱，共{weak_days}日出现'中性偏弱'信号。"
        else:
            structure_status = "近5日结构波动，信号方向交替。"
        
        if latest_capital in ['主力偏空', '主力净流出']:
            capital_status = f"资金流最近为'{latest_capital}'，需关注持续性。"
        elif latest_capital == '主力中性':
            capital_status = "资金流维持在'主力中性'。"
        else:
            capital_status = f"资金流为'{latest_capital}'。"
        
        volume_status = "量能维持中等水平，尚未出现明显失真。"
        
        report += f"""{structure_status} {capital_status} {volume_status}
"""
    else:
        report += "数据不足，无法进行近5日分析。\n"
    
    report += f"""---

## 4. 核心指标

- 锚定收益：{anchor_return*100:.2f}%({'大幅跑赢锚定基准' if anchor_return > 0.01 else '小幅跑赢锚定基准' if anchor_return > 0 else '小幅跑输锚定基准' if anchor_return > -0.01 else '大幅跑输锚定基准'})
- 相对强度：{relative_strength*100:.2f}%({'相对板块表现占优' if relative_strength > 0 else '相对板块处于劣势'})
- 研究层相对强度：{row['research_relative_strength']*100:.2f}%({'研究端偏多' if row['research_relative_strength'] > 0 else '研究端偏弱'})
- 估值标签：{row['valuation_label']}
- 资金流标签：{row['capital_flow_label']}
- 价格-资金关系：{row['price_capital_relation_label']}

---

## 5. 板块位置

暂无板块位置数据。

---

## 6. 今日关注信号

"""
    
    # 今日关注信号
    abnormal = row['abnormal_signals']
    if abnormal and abnormal != []:
        abnormal_str = '、'.join(abnormal) if isinstance(abnormal, list) else abnormal
        report += f"异常信号：{abnormal_str}。\n"
    else:
        if '偏强' in signal:
            report += f"暂无显著异常，主要表现为：{abnormal_str if abnormal else '价格强度维持强势'}。\n"
        else:
            report += f"暂无显著异常，主要表现为：价格强度走弱，相对强度转负。\n"
    
    report += f"""---

## 7. 今日可能驱动因素

- 市场整体情绪影响
- 主力资金态度变化
- 高估值标的承压

---

## 8. 研究层对比

"""
    
    # 研究层对比
    research_rs = row['research_relative_strength']
    if research_rs > 0.01:
        research_comment = f"研究层相对强度为{research_rs*100:.2f}%，与价格层的强势表现方向一致。"
    elif research_rs < -0.01:
        research_comment = f"研究层相对强度为{research_rs*100:.2f}%，与价格层的弱势表现一致。"
    else:
        research_comment = f"研究层相对强度为{research_rs*100:.2f}%，与价格层方向基本一致，但支撑力度有限。"
    
    if '偏强' in signal and research_rs > 0:
        conclusion = "短期信号与研究层视角均指向偏多，两者未出现背离。"
    elif '偏弱' in signal and research_rs < 0:
        conclusion = "短期信号与研究层视角均指向偏弱，两者未出现背离。"
    else:
        conclusion = "短期信号与研究层视角存在一定分歧。"
    
    report += f"""{research_comment} {conclusion}

---

## 9. 股票池复审提醒

暂无需要复审的标的。

---

## 10. 评分与反抽观察

评分层当前为{row['signal_score']:.2f}，评级'{row['signal_rating']}'，{'表明短期动能尚可' if row['signal_score'] > 0 else '表明短期动能不足'}。当前未触发反抽观察。

---

*日报生成于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
    
    return report

def save_report(content, trade_date):
    """保存报表到文件"""
    date_str = str(trade_date)
    filename = f"report_{date_str}.md"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return filepath

def main():
    df = load_data()
    
    # 获取命令行参数
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        # 默认最近交易日
        target_date = df.tail(1)['trade_date'].iloc[0]
    
    # 获取最近5日数据用于分析
    recent5 = get_recent_trading_days(df, 5)
    
    # 查找目标日期的数据
    target_row = df[df['trade_date'] == target_date]
    
    if target_row.empty:
        print(f"未找到日期 {target_date} 的数据")
        # 使用最近可用日期
        target_row = df.tail(1)
        target_date = target_row['trade_date'].iloc[0]
        print(f"使用最近可用日期: {target_date}")
    
    # 生成报表
    row = target_row.iloc[0]
    prev_rows = df[df['trade_date'] < target_date]
    
    content = generate_report_content(row, prev_rows)
    
    # 保存
    filepath = save_report(content, target_date)
    print(f"日报已生成: {filepath}")
    
    # 打印内容
    print("\n" + "="*50)
    print(content)

if __name__ == '__main__':
    main()
