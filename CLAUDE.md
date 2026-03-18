# Space Intel 项目指南

## 项目概述
商业航天板块每日复盘工具。自动获取市场数据、计算技术指标、生成 Markdown 格式的日报。

## 目录结构
```
space-intel/
├── config/           # 配置文件
│   └── stocks.json   # 股票池定义
├── data/
│   ├── raw/          # 原始数据 (AKShare 获取)
│   └── processed/    # 处理后的指标数据
├── reports/          # 每日复盘报告 (Markdown)
├── src/
│   ├── fetcher.py    # 数据获取模块
│   ├── storage.py    # 数据存储模块
│   ├── analyzer.py   # 指标计算模块
│   └── reporter.py   # 报告生成模块
├── CLAUDE.md         # 本文件
└── README.md         # 项目说明
```

## 核心模块

### fetcher.py
使用 AKShare 获取市场数据：
- 日K线数据（开高低收量）
- 板块指数数据

### storage.py
简单的文件存储：
- 原始数据存入 data/raw/
- 处理后数据存入 data/processed/

### analyzer.py
计算技术指标：
- RSI (相对强弱指数)
- MACD (指数平滑异同移动平均线)
- 布林带

### reporter.py
生成 Markdown 格式的每日复盘报告，保存到 reports/

## 技术栈
- Python 3.10+
- AKShare (数据源)
- pandas (数据处理)
- ta-lib 或 pandas-ta (技术指标)

## 开发规范
- 使用函数式编程，保持模块独立
- 配置与代码分离
- 每个模块可独立测试