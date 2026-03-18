# qclaude

围绕铂力特与商业航天股票池的交易研究工作台。

现在这套代码的核心不是“抓几条行情然后出日报”，而是两条独立数据链：

- 价格数据链：行情、`daily_basic`、`moneyflow`、价格数据产品、单日分析、连续观察、报告
- 新闻数据链：新闻源入口、新闻源注册、浏览器会话抓取、标准化事件、相关性分析

## 当前能力

- 构建价格数据产品 `data/processed/price_data_product.json`
- 基于价格底座生成 `daily_metrics.parquet`
- 计算近 5 日连续观察层
- 生成 Markdown 研究日报
- 维护极简新闻源入口 `config/news_sources.yaml`
- 用 Playwright + Edge 登录态抓取 Tushare 新闻页
- 将新闻按股票池边界分类为 `company_direct / pool_core / pool_extended / background / noise`

## 目录结构

```text
qclaude/
├── config/
│   ├── stocks.yaml
│   └── news_sources.yaml
├── scripts/
│   └── start_edge_news_debug.sh
├── src/
│   ├── fetcher.py
│   ├── normalizer.py
│   ├── price_data_product.py
│   ├── analyzer.py
│   ├── rolling_analyzer.py
│   ├── event_layer.py
│   ├── news_sources.py
│   ├── news_source_fetcher.py
│   ├── reporter.py
│   └── pipeline.py
└── tests/
```

## 安装

建议使用 Python 3.11+。

```bash
pip install -r requirements.txt
python3 -m pip install playwright
python3 -m playwright install chromium
```

## 环境变量

复制 `.env.example` 为 `.env`，并填入你自己的 Tushare token：

```bash
cp .env.example .env
```

`.env.example` 中的值只是占位符，不是可用 token。

## 股票池配置

主配置在 [config/stocks.yaml](/Users/pan/Desktop/02%20开发项目/qclaude/config/stocks.yaml)。

当前 schema 以这些层为主：

- `anchor`
- `core_universe`
- `research_core`
- `trading_candidates`
- `research_candidates`
- `extended_universe`

新闻相关性分析不会自己定义板块，而是直接以这份股票池作为业务边界。

## 价格数据链

价格链路顺序是：

1. `fetcher.py` 拉取 raw 数据
2. `normalizer.py` 标准化行情
3. `price_data_product.py` 构建价格数据产品
4. `analyzer.py` 生成单日指标
5. `rolling_analyzer.py` 生成近 5 日观察层
6. `reporter.py` 输出研究日报

主入口：

```bash
python3 src/pipeline.py
```

常用模式：

```bash
python3 src/pipeline.py --skip-fetch
python3 src/pipeline.py --skip-fetch --skip-events
```

约束规则：

- 价格链路失败，当日报告不出
- 新闻链路失败，不阻断价格主流程

## 新闻数据链

### 1. 新闻源入口

[config/news_sources.yaml](/Users/pan/Desktop/02%20开发项目/qclaude/config/news_sources.yaml) 只放链接：

```yaml
sources:
  - https://tushare.pro/news/cls
  - https://tushare.pro/news/eastmoney
```

系统会自动生成新闻源注册结果：

- `data/processed/news_sources_registry.json`

### 2. 浏览器抓取

Tushare 新闻页需要登录态。当前方案是：

- 用专用 Edge profile 持久保存登录态
- 用 Playwright 连接该浏览器会话
- 抓取真实新闻列表并落到 raw 层

启动专用浏览器：

```bash
./scripts/start_edge_news_debug.sh
```

第一次使用时：

1. 运行脚本
2. 在新开的 Edge 中登录 Tushare
3. 打开一次 `https://tushare.pro/news/cls` 或 `https://tushare.pro/news/eastmoney`

之后这份登录态会保存在：

- `~/.edge-codex-debug`

### 3. 原始抓取产物

抓取结果会写到：

- `data/raw/news_sources/latest.json`
- `data/raw/news_sources/tushare_cls.json`
- `data/raw/news_sources/tushare_eastmoney.json`

### 4. 新闻分析

[src/event_layer.py](/Users/pan/Desktop/02%20开发项目/qclaude/src/event_layer.py) 会把新闻标准化为事件，并结合股票池做相关性判断。

核心分层：

- `company_direct`
- `pool_core`
- `pool_extended`
- `background`
- `noise`

只有 `company_direct` 和 `pool_core` 会直接支撑更强的事件结论；弱相关和噪音只保留为背景。

## 测试

```bash
python3 -m pytest -q
```

如果你只想验证新闻抓取相关逻辑：

```bash
python3 -m pytest -q tests/test_news_sources.py tests/test_news_source_fetcher.py tests/test_event_layer_product.py
```

## Git 约定

仓库默认不提交本地运行产物：

- `data/`
- `archive/`
- `reports/`
- `daily-reports/`

这些目录已经在 `.gitignore` 中，适合作为本地缓存和分析输出。
