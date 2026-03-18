# 商业航天情报系统 & 催化剂新闻系统

## 项目简介

这是一个自动化金融情报系统，主要功能：
1. **商业航天板块复盘** - 每日分析铂力特（688393.SH）在商业航天板块中的相对强弱
2. **催化剂新闻监控** - 抓取并分析相关新闻，提取情绪、关联股票、事件类型等标签

---

## 项目结构

```
qclaude/
├── src/                    # 催化剂新闻系统（最新开发）
│   ├── catalyst_collector.py    # 主采集脚本
│   ├── ai_filter.py            # AI 过滤模块（情绪/关联股票/事件类型）
│   ├── news_by_stock.py        # 按股票聚合新闻
│   ├── news_alert.py           # 高影响力新闻预警
│   └── news_review.py          # 每日新闻复盘
├── config/
│   ├── catalyst_rules.yaml     # 催化剂规则配置
│   └── catalyst_sources.yaml   # 新闻源配置
├── data/
│   └── catalyst/               # 新闻数据存储
│       ├── news/               # 按日期存储的新闻 JSON
│       └── index/              # 股票索引
├── space-intel/               # 商业航天板块复盘系统（第一版）
│   ├── src/                   # 核心模块
│   └── ...
└── space-intel-mvp-plan.md    # MVP 方案文档
```

---

## 快速开始

### 环境准备

```bash
# 克隆后安装依赖
pip install -r requirements.txt

# 配置环境变量（可选）
cp .env.example .env
# 编辑 .env 填入 API 密钥
```

### 运行催化剂新闻采集

```bash
# 采集今日新闻
python -m src.catalyst_collector

# 按股票查询新闻
python -m src.news_by_stock 航天动力 --days 7

# 构建股票索引
python -m src.news_by_stock --build-index

# 查看高影响力利好新闻
python -m src.news_alert --sentiment 利好 --impact high

# 复盘指定日期新闻
python -m src.news_review 20260318
```

### 商业航天板块复盘

```bash
cd space-intel
pip install -r requirements.txt
python daily_report.py
```

---

## 依赖

主要依赖（见 `requirements.txt`）：
- `requests` - HTTP 请求
- `pyyaml` - YAML 配置
- `openai` - OpenAI API（用于新闻分析）
- `tushare` - 股票数据（可选）

---

## 更新日志

### 2026-03-18 Phase 3 更新

**新增功能：**
- AI 智能过滤：自动提取新闻的情绪（利好/中性/利空）、关联股票、事件类型
- 按股票聚合新闻：支持查询特定股票的相关新闻
- 高影响力新闻预警：筛选重要新闻
- 每日新闻复盘：支持日期对比和近期汇总

**新增文件：**
- `src/ai_filter.py` - AI 过滤模块
- `src/news_by_stock.py` - 按股票聚合
- `src/news_alert.py` - 预警脚本
- `src/news_review.py` - 复盘脚本

**数据标签：**
| 字段 | 值 | 说明 |
|------|-----|------|
| `sentiment` | 利好/中性/利空 | 情绪分析 |
| `related_stocks` | ["航天动力"] | 关联股票 |
| `event_type` | 政策/公告/技术/订单/澄清/事件 | 事件分类 |

---

## 在其他电脑继续工作

```bash
# 1. 克隆仓库
git clone https://github.com/notepi/qclaude.git
cd qclaude

# 2. 创建虚拟环境（推荐）
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# venv\Scripts\activate   # Windows

# 3. 安装依赖
pip install -r requirements.txt

# 4. 复制并配置环境变量
cp .env.example .env
# 编辑 .env 填入必要的 API 密钥

# 5. 运行测试
python -m src.catalyst_collector
```

---

## 配置说明

### catalyst_rules.yaml

关键配置项：
- `stock_pool` - 关注的股票池
- `event_types` - 事件类型分类
- `investment_themes` - 投资主题关键词

### 环境变量

```bash
# OpenAI API（用于 AI 过滤）
OPENAI_API_KEY=sk-...

# Tushare（股票数据，可选）
TUSHARE_TOKEN=...
```

---

## License

MIT
