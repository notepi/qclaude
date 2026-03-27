# Space Intel 项目指南

## 项目概述
商业航天板块每日复盘工具。自动获取市场数据、计算技术指标、生成 Markdown 格式的日报。

## 架构

系统分为三个独立模块：

```
src/
├── price/           # 行情数据线（产出 85 个指标）
├── news/            # 新闻数据线
├── dailyreport/     # 日报模块（格式化输出）
└── shared/          # 共享工具
```

## 运行命令

```bash
# 完整运行
uv run python scripts/run_all.py

# 独立运行各模块
uv run python -m src.price.run       # 行情数据线
uv run python -m src.news.run        # 新闻数据线
uv run python -m src.dailyreport.run # 日报生成
```

## 目录结构

```
space-intel/
├── config/                    # 配置文件
│   ├── stocks.yaml           # 股票池定义
│   ├── news_sources.yaml     # 新闻源配置
│   ├── catalyst_rules.yaml   # 催化筛选规则
│   └── catalyst_sources.yaml # 催化源配置
│
├── data/                      # 共享存储层
│   ├── price/                # 行情数据
│   │   ├── raw/              # 原始数据 (market_data, daily_basic, moneyflow)
│   │   ├── normalized/       # 标准化数据 (market_data_normalized)
│   │   ├── processed/        # 处理后数据 (daily_metrics, price_data_product)
│   │   ├── analytics/        # 分析数据 (rolling_metrics, scored_metrics)
│   │   └── archive/          # 归档数据 (YYYYMMDD.parquet)
│   ├── news/                 # 新闻数据
│   │   ├── raw/news_sources/ # 按来源存储 (tushare_cls, tushare_eastmoney)
│   │   └── processed/        # news_sources_registry, daily_events
│   └── catalyst/             # 催化数据
│       ├── index/            # stocks.json
│       └── news/             # 催化新闻数据
│
├── src/
│   ├── price/                # 行情数据线
│   │   ├── fetcher.py        # 数据获取
│   │   ├── normalizer.py     # 数据标准化
│   │   ├── analyzer.py       # 核心指标计算 v3.1
│   │   ├── rolling_analyzer.py # 连续观察指标 v2.7
│   │   ├── diagnosis_layer.py # 诊断层 v1.1（新增）
│   │   ├── score_layer.py    # 多标签评分
│   │   ├── rebound_watch_layer.py # 反抽观察
│   │   ├── data_product.py   # 数据产品接口
│   │   └── run.py            # 模块入口（8步流程）
│   │
│   ├── news/                 # 新闻数据线
│   │   ├── news_sources.py   # 新闻源管理
│   │   ├── ai_filter.py      # AI 智能筛选
│   │   ├── event_layer.py    # 事件提取 v1.0
│   │   ├── data_product.py   # 数据产品接口
│   │   └── run.py            # 模块入口
│   │
│   ├── dailyreport/          # 日报模块
│   │   ├── reporter.py       # 生成 Markdown 报告 v2.7（瘦身版）
│   │   ├── review_stock_pool.py # 股票池复审提醒
│   │   ├── data_product.py   # 数据产品接口
│   │   └── run.py            # 模块入口
│   │
│   ├── shared/               # 共享工具
│   │   ├── config.py         # 配置加载
│   │   ├── storage.py        # 统一存储层访问
│   │   └── paths.py          # 路径常量定义
│   │
│   ├── backfill.py           # 历史数据回填
│   ├── evaluate_stock.py     # 股票池评估
│   ├── validate_signals.py   # 信号验证
│   └── explain_signal_state.py # 状态解释
│
├── docs/                     # 文档
│   ├── field_glossary.md    # 数据线指标字典（v3.1）
│   └── pool_governance.md   # 股票池治理规范
│
├── reports/                  # 生成的报告
└── scripts/run_all.py        # 统一入口
```

## 核心模块

### 行情数据线 (src/price/)

8 步流程，产出 85 个指标：

1. **fetcher.py** - 从 Tushare 获取股票数据
2. **normalizer.py** - 数据标准化
3. **data_product.py** - 构建价格数据产品
4. **analyzer.py** - 核心指标计算（涨跌幅、相对强弱、资金流向、估值）v3.1
5. **rolling_analyzer.py** - 连续观察指标（近5日价格/量能/资金连续性）v2.7
6. **score_layer.py** - 多标签评分
7. **diagnosis_layer.py** - 诊断层（综合诊断、信号拆解、观察清单）v1.1
8. **rebound_watch_layer.py** - 反抽观察信号

### 新闻数据线 (src/news/)

3 步流程：

- `run.py` - 模块入口
- `event_layer.py` - 事件提取（Akshare/巨潮）
- `ai_filter.py` - AI 智能筛选（可选）

### 日报模块 (src/dailyreport/)

v2.7 瘦身版：

- `reporter.py` - 格式化输出，不做计算（~800 行）
- 所有诊断逻辑下沉到数据线

### 共享模块 (src/shared/)

- `config.py` - 配置加载工具
- `storage.py` - 统一存储层访问
- `paths.py` - 路径常量定义

## 数据线指标库

数据线产出 **85 个字段**：

| 数据源 | 字段数 | 说明 |
|--------|--------|------|
| daily_metrics | 60 | 当日指标（行情、资金、估值、标签） |
| rolling_metrics | 24 | 连续观察指标（近5日连续性） |
| diagnosis | 11 | 诊断指标（综合诊断、信号拆解） |

**完整字段说明**：[docs/field_glossary.md](docs/field_glossary.md)

## 技术栈

- Python 3.13+
- Tushare (股票数据源)
- pandas (数据处理)
- Dashscope (新闻 AI 筛选，可选)

## 开发规范

- 使用函数式编程，保持模块独立
- 配置与代码分离
- 每个模块可独立测试
- **reporter 只做格式化输出，不做计算**（v2.7 架构原则）
- 通过数据产品接口解耦模块间依赖
- 使用 `src.shared.storage.Storage` 类访问数据目录

## 项目文档

- `ARCHITECTURE.md` - 系统架构详细说明
- `docs/field_glossary.md` - 数据线指标字典
- `docs/pool_governance.md` - 股票池治理规范