# 数据线指标字典（Field Glossary）

> 版本：v3.1 | 最后更新：2026-03-26
> 本文档是系统所有数据线字段的唯一口径说明。新增字段时必须同步更新此文档。

---

## 一、daily_metrics（当日指标）

### 1.1 基础行情

| 字段 | 类型 | 单位 | 含义 |
|------|------|------|------|
| `trade_date` | datetime64 | - | 交易日期 |
| `anchor_symbol` | str | - | 锚定标的代码，如 `688333.SH` |
| `anchor_return` | float | 小数 | 当日涨跌幅，如 `-0.0109` |
| `sector_avg_return` | float | 小数 | 板块平均涨跌幅（core_universe 均值，不含 anchor） |
| `relative_strength` | float | 小数 | 相对强弱 = anchor_return - sector_avg_return |
| `anchor_amount` | float | 千元 | 当日成交额（Tushare 原始单位） |
| `amount_20d_high` | bool | - | 是否创20日成交额新高 |
| `amount_vs_5d_avg` | float | 倍 | 当日成交额 / 前5日均值，如 `0.97` |
| `core_universe_count` | int | - | 实际参与板块均值计算的股票数 |
| `sector_total_count` | int | - | core_universe 配置总数 |
| `sector_total_size` | int | - | 参与排名的样本总数（core_universe + anchor） |

### 1.2 板块排名

| 字段 | 类型 | 含义 |
|------|------|------|
| `return_rank_in_sector` | int | 涨跌幅排名（第1=最高），样本含 anchor |
| `amount_rank_in_sector` | int | 成交额排名（第1=最高），样本含 anchor |

### 1.3 基本面指标（v2.4）

| 字段 | 类型 | 单位 | 含义 |
|------|------|------|------|
| `pe_ttm` | float | 倍 | 市盈率 TTM |
| `pb` | float | 倍 | 市净率 |
| `ps_ttm` | float | 倍 | 市销率 TTM |
| `total_mv` | float | 万元 | 总市值 |
| `circ_mv` | float | 万元 | 流通市值 |
| `turnover_rate` | float | % | 换手率 |
| `turnover_rate_f` | float | % | 自由流通换手率 |

### 1.4 资金流向指标（v2.4）

| 字段 | 类型 | 单位 | 含义 |
|------|------|------|------|
| `net_mf_amount` | float | 万元 | 主力净流入（正=流入，负=流出） |
| `buy_elg_vol` | float | 手 | 超大单买入量 |
| `sell_elg_vol` | float | 手 | 超大单卖出量 |
| `buy_lg_vol` | float | 手 | 大单买入量 |
| `sell_lg_vol` | float | 手 | 大单卖出量 |
| `buy_elg_amount` | float | 万元 | 超大单买入额 |
| `sell_elg_amount` | float | 万元 | 超大单卖出额 |
| `buy_lg_amount` | float | 万元 | 大单买入额 |
| `sell_lg_amount` | float | 万元 | 大单卖出额 |
| `buy_md_amount` | float | 万元 | 中单买入额 |
| `sell_md_amount` | float | 万元 | 中单卖出额 |
| `buy_sm_amount` | float | 万元 | 小单买入额 |
| `sell_sm_amount` | float | 万元 | 小单卖出额 |

### 1.5 状态标签（v2.5）

| 字段 | 取值 | 含义 |
|------|------|------|
| `price_strength_label` | 强/中/弱 | 价格强度标签 |
| `volume_strength_label` | 强/中/弱 | 成交额强度标签 |
| `overall_signal_label` | 强/中性偏强/中性/中性偏弱/弱 | 综合信号标签 |
| `abnormal_signals` | list[str] | 异常信号列表，如 `["涨幅居板块首位"]` |
| `valuation_label` | 高估值/中性偏高/中性估值/低估值 | 估值标签 |
| `capital_flow_label` | 主力偏多/主力偏空/主力中性 | 资金流向标签 |
| `activity_label` | 活跃/正常/低活跃 | 活跃度标签 |

### 1.6 资金结构标签（v2.5B）

| 字段 | 类型 | 含义 |
|------|------|------|
| `retail_order_net` | float | 万元 | 中小资金净流入 |
| `big_order_ratio` | float | 0~1 | 大资金成交占比 |
| `capital_structure_label` | str | 资金结构标签：大资金主导买入/大资金主导卖出/中小资金主导/资金分歧/方向不明 |
| `price_capital_relation_label` | str | 价格资金关系：上行配合/下行配合/上涨背离/下跌背离/中性 |

**标签规则**：
- `大资金主导买入`：大资金占比>50% 且净流入
- `大资金主导卖出`：大资金占比>50% 且净流出
- `中小资金主导`：大资金占比<30%
- `资金分歧`：大资金和中小资金方向相反
- `上涨背离`：价格上涨但主力净流出
- `下跌背离`：价格下跌但主力净流入

### 1.7 研究层对比（v2.6）

| 字段 | 类型 | 含义 |
|------|------|------|
| `research_avg_return` | float | 小数 | 研究层（research_core）平均涨跌幅 |
| `research_relative_strength` | float | 小数 | 相对研究层的强弱 |

**说明**：研究层是产业链核心环节对标，与交易层（trading_core）口径不同。

### 1.8 多维度估值（v3.1）

| 字段 | 类型 | 含义 |
|------|------|------|
| `valuation_score` | float | 综合估值评分（-2~+2，负=低估） |
| `valuation_detail` | str | 估值细节描述，如 "PE板块偏低+历史低位，PB板块偏低" |
| `sector_pe_mean` | float | 板块 PE 均值 |
| `sector_pb_mean` | float | 板块 PB 均值 |
| `sector_ps_mean` | float | 板块 PS 均值 |
| `pe_vs_sector` | float | 小数 | PE 相对板块偏差（负=低于板块） |
| `pb_vs_sector` | float | 小数 | PB 相对板块偏差 |
| `ps_vs_sector` | float | 小数 | PS 相对板块偏差 |
| `pe_sector_position` | str | PE 板块位置：偏高/适中/偏低 |
| `pb_sector_position` | str | PB 板块位置 |
| `ps_sector_position` | str | PS 板块位置 |
| `pe_percentile_60d` | float | 0~100 | PE 近60日历史分位 |
| `pb_percentile_60d` | float | 0~100 | PB 近60日历史分位 |
| `ps_percentile_60d` | float | 0~100 | PS 近60日历史分位 |
| `pe_percentile_label` | str | 分位标签：高位/中位偏上/中位/中位偏下/低位 |

---

## 二、rolling_metrics（连续观察指标）

### 2.1 价格连续性

| 字段 | 类型 | 含义 |
|------|------|------|
| `rs_outperform_days_5d` | int | 近N日跑赢板块天数（rs > 0） |
| `rs_consecutive_outperform` | int | 连续跑赢天数（从最新日往前数） |
| `rs_consecutive_underperform` | int | 连续跑输天数 |
| `rs_5d_mean` | float | 小数 | 近N日相对强弱均值 |
| `rs_5d_series` | list[float] | 相对强弱序列（旧→新），如 `[-0.009, 0.001, 0.004, -0.032, 0.006]` |

### 2.2 量能连续性

| 字段 | 类型 | 含义 |
|------|------|------|
| `volume_expand_days_5d` | int | 放量天数（amount_vs_5d_avg > 1.0） |
| `amount_20d_high_days_5d` | int | 成交额创20日新高天数 |
| `volume_consecutive_shrink` | int | 连续缩量天数（< 0.8倍） |
| `volume_consecutive_expand` | int | 连续放量天数（> 1.2倍） |
| `amount_vs_5d_avg_series` | list[float] | 成交额倍数序列（旧→新） |

### 2.3 资金连续性

| 字段 | 类型 | 单位 | 含义 |
|------|------|------|------|
| `mf_inflow_days_5d` | int | - | 近N日主力净流入天数 |
| `mf_consecutive_inflow` | int | - | 连续净流入天数 |
| `mf_consecutive_outflow` | int | - | 连续净流出天数 |
| `mf_5d_mean` | float | 万元 | 近N日主力净流入均值 |
| `capital_flow_trend_label` | str | - | 资金趋势标签 |

**资金趋势标签取值**：
- `资金持续流入`：净流入天数≥4 且 均值>500万
- `资金持续流出`：连续净流出≥3 或 净流出天数≥4且均值<-500万
- `资金开始回流`：前几日持续流出，今日转正
- `资金开始转弱`：前几日持续流入，今日转负
- `资金状态中性`：其他情况

### 2.4 综合标签

| 字段 | 取值 | 含义 |
|------|------|------|
| `price_trend_label` | 连续强于板块/连续弱于板块/短期相对强势/短期走弱/短期中性 | 价格趋势标签 |
| `volume_trend_label` | 持续放量/持续缩量/放量增强/缩量企稳/量能平稳 | 量能趋势标签 |
| `momentum_label` | 量价齐升/价强量稳/价强量弱/量价齐弱/放量下跌/短期震荡 | 综合动量标签 |

**动量标签矩阵**：
```
         量强    量稳    量弱
价强   量价齐升  价强量稳  价强量弱
价弱   放量下跌  短期震荡  量价齐弱
```

### 2.5 摘要文本

| 字段 | 类型 | 含义 |
|------|------|------|
| `trend_summary` | str | 规则化短周期摘要（2~3句） |
| `rolling_summary_text` | str | 压缩版近N日结构摘要，如 "跑赢板块 4/5 日，胜率较高" |

---

## 三、diagnosis（诊断指标）

### 3.1 诊断结果

| 字段 | 类型 | 含义 |
|------|------|------|
| `diagnosis_label` | str | 综合诊断标签，如 "中性，暂未形成明确方向" |
| `diagnosis_reasons` | list[str] | 诊断原因列表（最多3条），如 `["近5日结构未坏", "主力未确认回流"]` |
| `next_watch` | list[str] | 明日观察清单（最多4条），如 `["主力是否转正", "成交额放大"]` |

### 3.2 信号拆解

| 字段 | 类型 | 含义 |
|------|------|------|
| `signal_breakdown` | dict | 信号拆解对象 |
| `signal_breakdown.abnormal` | list[str] | 异常信号，如 `["跑赢板块明显"]` |
| `signal_breakdown.positive` | list[str] | 积极信号，如 `["近5日结构未坏", "研究层相对更强"]` |
| `signal_breakdown.risk` | list[str] | 风险信号，如 `["主力资金流出", "主力未确认回流"]` |

### 3.3 资金解读文本

| 字段 | 类型 | 含义 |
|------|------|------|
| `capital_summary_text` | str | 主力总体描述，如 "净流出 2882 万元，整体偏空" |
| `elg_action_text` | str | 超大单行为描述，如 "净买入 3236 手，盘中存在局部承接" |
| `participation_text` | str | 大资金参与度描述，如 "25.3%，参与度偏低，未形成主导" |
| `capital_conclusion_text` | str | 资金综合结论，如 "有承接但力度不足，不足以扭转弱势" |

### 3.4 近5日摘要（含今日）

| 字段 | 类型 | 含义 |
|------|------|------|
| `rolling_summary_text` | str | 含今日数据的近5日摘要，如 "跑赢板块 4/5 日，平均相对强弱 -0.39%，属于\"胜率较高\"；量能量能平稳；资金面有改善迹象" |

---

## 四、事件层字段（来源：event_layer）

### `announcement_status / company_news_status / sector_news_status`
- **取值**：
  - `ok`：成功获取，结果可信
  - `empty`：接口正常，确认当日无数据
  - `unavailable`：接口暂不可用（已知问题）
  - `timeout`：请求超时
  - `permission_denied`：无接口权限
  - `error`：其他未知异常
- **关键原则**：status 非 `ok`/`empty` 时，reporter 不得写"今日无XX"，必须写"未获取到有效信息"

### `event_signal_label`
- **取值**：`有明确催化 / 有弱催化 / 无明确催化`
- **规则**：
  - 有明确催化：`announcement_status=ok` 且公告≥1条，或公司新闻≥2条
  - 有弱催化：公司新闻=1条，或板块新闻≥1条
  - 无明确催化：公司新闻和板块新闻均为空

---

## 五、口径变更记录

| 日期 | 版本 | 变更内容 |
|------|------|------|
| 2026-03-17 | v2.1.1 | 确认 amount 单位为千元；修复 reporter 误当万元的展示错误 |
| 2026-03-17 | v2.1 | 明确 return_rank_in_sector 排名样本含 anchor |
| 2026-03-17 | v2.2.1 | event_signal_label 收紧规则 |
| 2026-03-18 | v2.4 | 新增基本面指标、资金流向指标 |
| 2026-03-19 | v2.5 | 新增状态标签、资金结构标签 |
| 2026-03-20 | v2.5B | 新增 capital_structure_label、price_capital_relation_label |
| 2026-03-24 | v2.6 | 新增研究层对比字段、rolling_summary_text |
| 2026-03-25 | v3.1 | 新增多维度估值字段 |
| 2026-03-26 | v3.1+ | 新增诊断层字段（diagnosis_label、signal_breakdown 等） |

---

## 六、单位速查表

| 字段类型 | 单位 | 示例 |
|----------|------|------|
| 成交额（raw） | 千元 | amount, anchor_amount |
| 资金流向 | 万元 | net_mf_amount, retail_order_net |
| 成交量 | 手 | vol, buy_elg_vol |
| 市值 | 万元 | total_mv, circ_mv |
| 涨跌幅 | 小数 | anchor_return, relative_strength |
| 比例/占比 | 小数（0~1） | big_order_ratio |
| 换手率 | 百分比（%） | turnover_rate |
| 分位数 | 0~100 | pe_percentile_60d |