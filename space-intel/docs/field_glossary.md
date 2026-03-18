# 字段口径字典（Field Glossary）

> 版本：v2.3 | 最后更新：2026-03-17  
> 本文档是系统所有关键字段的唯一口径说明。新增字段时必须同步更新此文档。

---

## 一、行情数据字段（来源：Tushare）

### `ts_code`
- **含义**：股票代码，含交易所后缀
- **格式**：`{6位数字}.{SH|SZ}`，如 `688333.SH`
- **存储口径**：字符串，含后缀
- **注意**：akshare 接口使用纯数字代码（`688333`），调用时需去掉后缀；存储时统一还原为含后缀格式

---

### `trade_date`
- **含义**：交易日期
- **Tushare 原始格式**：`YYYYMMDD` 字符串，如 `"20260316"`
- **存储口径（parquet）**：`datetime64[ns]`，由 analyzer 在读取时转换
- **展示口径**：`YYYY-MM-DD`，如 `2026-03-16`
- **注意**：从 parquet 读取后为 `pd.Timestamp`，格式化时需显式调用 `.strftime('%Y-%m-%d')`；不要直接 `str()` 转换

---

### `open / high / low / close`
- **含义**：当日开盘价 / 最高价 / 最低价 / 收盘价
- **单位**：元（人民币）
- **存储口径**：float，元
- **展示口径**：元，保留2位小数
- **注意**：当前未做复权处理；长期回溯时需注意除权除息影响

---

### `vol`
- **含义**：成交量
- **单位**：手（1手 = 100股）
- **存储口径**：float，手
- **展示口径**：万手（÷10000），保留2位小数
- **注意**：Tushare 单位是手，不是股；不要误当成股数使用

---

### `amount`
- **含义**：成交额
- **单位**：⚠️ **千元**（Tushare 原始单位）
- **存储口径**：float，千元（保持 Tushare 原始值，不做转换）
- **展示口径**：
  - `amount / 10` → 万元
  - `amount / 100_000` → 亿元
  - 展示规则：≥10000万元（即≥1亿）时显示亿元，否则显示万元
- **⚠️ 高风险字段**：已出现过一次单位误用（千元误当万元，导致显示值偏大10倍）
- **原则**：存储层永远保持千元原始值；单位转换只在 `reporter.py` 的 `fmt_amount()` 中进行

---

## 二、分析指标字段（来源：analyzer）

### `anchor_symbol`
- **含义**：锚定标的股票代码
- **格式**：同 `ts_code`，如 `688333.SH`
- **来源**：`config/stocks.yaml` → `anchor.code`
- **约束**：必须与 `core_universe` 严格互斥；不参与板块均值计算；不参与排名时的均值分母

---

### `anchor_return`
- **含义**：锚定标的当日涨跌幅
- **计算**：`(close_t - close_{t-1}) / close_{t-1}`
- **单位**：小数（如 `-0.00376`）
- **展示口径**：百分比，带符号（如 `-0.38%`）
- **注意**：使用前一交易日收盘价，不是开盘价

---

### `sector_avg_return`
- **含义**：板块平均涨跌幅
- **计算**：`core_universe` 中当日有效数据的股票涨跌幅算术平均
- **口径**：**不含 anchor_symbol**；不含 extended_universe
- **单位**：小数
- **注意**：参与计算的股票数可能少于 core_universe 总数（部分股票当日无数据）；实际参与数记录在 `core_universe_count`

---

### `relative_strength`
- **含义**：锚定标的相对板块强弱
- **计算**：`anchor_return - sector_avg_return`
- **单位**：小数（如 `0.01457`）
- **展示口径**：百分比，带符号（如 `+1.46%`）
- **注意**：正值表示跑赢板块，负值表示跑输板块；分母是板块均值，不是指数

---

### `anchor_amount`
- **含义**：锚定标的当日成交额
- **单位**：⚠️ **千元**（继承自 raw 层，不做转换）
- **展示口径**：同 `amount` 字段

---

### `amount_20d_high`
- **含义**：当日成交额是否为近20个交易日最高值
- **类型**：bool
- **计算**：`anchor_amount == max(最近20日 amount)`
- **注意**：数据不足20日时，与历史全部数据比较

---

### `amount_vs_5d_avg`
- **含义**：当日成交额相对前5日均值的倍数
- **计算**：`amount_t / mean(amount_{t-5} ... amount_{t-1})`
- **单位**：倍数（如 `0.72`）
- **展示口径**：`0.72x`
- **注意**：分母是**前5日**（不含当日）；数据不足6日时返回 None

---

### `core_universe_count`
- **含义**：当日实际参与板块均值计算的股票数
- **类型**：int
- **注意**：可能小于 `sector_total_count`（部分股票当日停牌或无数据）

---

### `sector_total_count`
- **含义**：`core_universe` 配置中的股票总数（不含 anchor）
- **类型**：int
- **来源**：`config/stocks.yaml` → `core_universe` 列表长度

---

### `return_rank_in_sector`
- **含义**：锚定标的涨跌幅在板块中的排名（第1名=最高）
- **类型**：int
- **排名样本**：`core_universe` 有效数 + anchor（**含 anchor**）
- **⚠️ 口径注意**：排名样本**含 anchor**，与均值计算（不含 anchor）口径不同
- **约束**：`1 <= return_rank_in_sector <= sector_total_size`

---

### `amount_rank_in_sector`
- **含义**：锚定标的成交额在板块中的排名（第1名=最高）
- **类型**：int
- **排名样本**：同 `return_rank_in_sector`，含 anchor
- **约束**：`1 <= amount_rank_in_sector <= sector_total_size`

---

### `sector_total_size`
- **含义**：参与排名的样本总数
- **计算**：`core_universe 有效参与数 + 1（anchor）`
- **注意**：与 `sector_total_count` 不同；`sector_total_count` 是配置总数，`sector_total_size` 是实际参与排名的数量

---

## 三、标签字段（来源：analyzer）

### `price_strength_label`
- **取值**：`强 / 中 / 弱`
- **规则**：
  - 强：`anchor_return > 2%` 或（`relative_strength > 1%` 且 `return_rank <= total/2`）
  - 弱：`anchor_return < -2%` 或（`relative_strength < -1%` 且 `return_rank > total/2`）
  - 中：其余

### `volume_strength_label`
- **取值**：`强 / 中 / 弱`
- **规则**：
  - 强：`amount_20d_high=True` 或 `amount_vs_5d_avg > 1.5` 或 `amount_rank <= total/2`
  - 弱：`amount_vs_5d_avg < 0.7` 且 `amount_20d_high=False` 且 `amount_rank > total/2`（三条全满足）
  - 中：其余

### `overall_signal_label`
- **取值**：`强 / 中性偏强 / 中性 / 中性偏弱 / 弱`
- **规则**：价格强度 × 成交额强度矩阵，见 `analyzer.py`

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
- **⚠️ 关键原则**：status 非 `ok`/`empty` 时，reporter 不得写"今日无XX"，必须写"未获取到有效信息"

### `event_signal_label`
- **取值**：`有明确催化 / 有弱催化 / 无明确催化`
- **规则（v2.2.1）**：
  - 有明确催化：`announcement_status=ok` 且公告≥1条，或公司新闻≥2条
  - 有弱催化：公司新闻=1条，或板块新闻≥1条
  - 无明确催化：公司新闻和板块新闻均为空

---

## 五、口径变更记录

| 日期 | 字段 | 变更内容 |
|---|---|---|
| 2026-03-17 | `amount` | 确认 Tushare 原始单位为千元；修复 reporter 中误当万元的展示错误（v2.1.1）|
| 2026-03-17 | `return_rank_in_sector` | 明确排名样本含 anchor，与均值口径不同（v2.1）|
| 2026-03-17 | `event_signal_label` | 收紧规则：公告纳入判断需 status=ok；排名信号改为 rank=1/2 分级触发（v2.2.1）|
