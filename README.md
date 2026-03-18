# Space Intel

商业航天板块每日复盘工具。

## 功能

- 自动获取商业航天板块股票数据
- 计算技术指标 (RSI, MACD, 布林带)
- 生成 Markdown 格式的每日复盘报告

## 安装

```bash
pip install akshare pandas pandas-ta
```

## 使用

```bash
python -m src.main
```

## 股票池配置

配置文件：`config/stocks.yaml`

### 结构说明

```yaml
anchor_symbol: 688333.SH  # 核心锚定标的

commercial_space_universe:  # 商业航天股票池
  - code: ...
    name: ...
    tags: [category, ...]

reference_indices:  # 参考指数
  - code: ...
    name: ...
```

### 核心标的

| 代码 | 名称 | 定位 |
|------|------|------|
| 688333.SH | 铂力特 | 锚定标的，板块风向标 |

### 股票池分类

- **增材制造**: 铂力特（核心）
- **卫星制造**: 中国卫星
- **航天配套**: 航天电子、航天动力、航天工程、中航光电
- **电子元器件**: 火炬电子、振华科技、鸿远电子
- **遥感/GIS**: 航天宏图、中科星图

## 项目结构

```
space-intel/
├── config/           # 配置文件
├── data/
│   ├── raw/          # 原始数据
│   └── processed/    # 处理后数据
├── reports/          # 每日报告
└── src/              # 源代码
    ├── fetcher.py    # 数据获取
    ├── storage.py    # 数据存储
    ├── analyzer.py   # 指标计算
    └── reporter.py   # 报告生成
```