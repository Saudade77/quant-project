# 多因子选股与回测系统

[English Version](README.md)

这是一个面向量化研究 / 量化策略实习简历展示的 Python 项目，目标是搭建一个小而完整的研究闭环，覆盖：

- 市场与基本面数据准备
- 横截面因子构建
- 月频组合选股
- 日频回测
- IC、分组收益和绩效分析

当前版本同时支持样例数据和免费真实数据方案：

- 价格与基准通过 `yfinance` 从 Yahoo Finance 获取
- 基本面通过 SEC EDGAR 原始数据派生

## 项目能力

当前仓库已经支持：

- 本地 `csv/parquet` 数据加载
- 价格、基准、基本面数据标准化
- 股票池过滤
- 因子计算：
  - `bp`
  - `ep`
  - `mom_20`
  - `mom_60`
  - `roe`
- 因子预处理与综合打分
- 月频调仓和等权组合构建
- 日频 close-to-close 回测
- 绩效指标计算：
  - `total_return`
  - `annual_return`
  - `annual_volatility`
  - `sharpe`
  - `max_drawdown`
- IC / RankIC 分析
- 分组收益分析
- 结果导出到 `outputs/reports/`

## 研究流程

```text
价格 + 基本面 + 基准
  -> 数据加载与清洗
  -> 股票池过滤
  -> 因子计算
  -> 因子预处理
  -> 多因子打分
  -> 组合构建
  -> 回测
  -> 分析与报告导出
```

## 项目结构

```text
quant-project/
├── config/
├── data/
│   ├── raw/
│   └── sample/
├── outputs/
│   └── reports/
├── scripts/
│   ├── fetch_sec_data.py
│   ├── make_sample_data.py
│   ├── prepare_real_data.py
│   ├── prepare_sec_fundamentals.py
│   ├── prepare_yahoo_prices.py
│   ├── run_backtest.py
│   └── run_pipeline.py
├── src/
│   ├── analysis/
│   ├── backtest/
│   ├── data/
│   ├── factors/
│   ├── strategy/
│   └── utils/
└── tests/
```

## 环境安装

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 如何运行

### 方式 1：样例数据

如果标准数据表不存在，主流程会自动回退到样例数据：

```bash
python3 scripts/run_pipeline.py
```

### 方式 2：免费真实数据

可以直接使用仓库内置的大盘股 starter universe：

```bash
python3 scripts/prepare_real_data.py \
  --tickers-file data/sample/us_largecap_tickers.txt \
  --start-date 2020-01-01 \
  --end-date 2025-12-31 \
  --benchmark-ticker SPY \
  --sec-user-agent "your-name your-email@example.com"
```

准备完成后运行：

```bash
python3 scripts/run_pipeline.py
```

如果真实数据准备成功，终端应显示：

```text
Input data source: existing_project_data
```

如果只想看回测摘要：

```bash
python3 scripts/run_backtest.py
```

## 数据来源

### 价格与基准

- 使用 `yfinance` 从 Yahoo Finance 获取
- 基准默认使用 `SPY`

准备脚本：

```bash
python3 scripts/prepare_yahoo_prices.py \
  --tickers-file data/sample/us_largecap_tickers.txt \
  --start-date 2020-01-01 \
  --end-date 2025-12-31 \
  --benchmark-ticker SPY
```

### SEC 原始基本面

项目会下载以下 SEC 数据：

- `company_tickers.json`
- `submissions.zip`
- `companyfacts.zip`

准备脚本：

```bash
python3 scripts/fetch_sec_data.py \
  --user-agent "your-name your-email@example.com"
```

### 标准化基本面表

项目最终会派生出研究使用的标准基本面表，字段包括：

- `date`
- `ticker`
- `pe`
- `pb`
- `roe`

准备脚本：

```bash
python3 scripts/prepare_sec_fundamentals.py
```

## 三张标准输入表

主流程只读取这三张标准表：

- `data/raw/us_equity_prices.parquet`
- `data/raw/us_equity_fundamentals.parquet`
- `data/raw/us_benchmark.parquet`

这样可以把研究主流程和在线下载、原始数据清洗分开。

## 输出结果

运行 `python3 scripts/run_pipeline.py` 后，默认会生成：

- `outputs/reports/scores.csv`
- `outputs/reports/portfolio.csv`
- `outputs/reports/backtest_result.csv`
- `outputs/reports/ic_result.csv`
- `outputs/reports/group_return_result.csv`
- `outputs/reports/analysis_report.txt`

## 配置说明

主要配置文件：

- `config/default.yaml`
- `config/paths.yaml`

当前默认设置包括：

- 市场：`us`
- 基准：`SPY`
- 因子：`bp`、`ep`、`mom_20`、`mom_60`、`roe`
- 调仓频率：`monthly`
- 组合规模：`top_n = 20`
- 权重方式：`equal`
- 成交价格：`close`
- 手续费：`10 bps`

## 内置股票池

仓库中包含一份 starter ticker 列表：

- `data/sample/us_largecap_tickers.txt`

这份列表适合真实数据的第一轮测试：

- 以大盘股为主
- 行业覆盖较广
- Yahoo / SEC 覆盖相对稳定
- 比全市场股票池更容易控制数据清洗复杂度

## 当前限制

- 仅支持日频研究
- 交易成本模型较简化
- 默认 close-to-close 回测
- SEC 财务对齐是研究级近似，不是机构级 point-in-time 系统
- 银行和保险公司不是第一期重点适配对象

## 使用声明

本仓库仅用于研究和学习。项目中的 Yahoo Finance 数据通过 `yfinance` 获取，基本面数据来自公开的 SEC EDGAR 资源。在用于其他场景前，请自行确认各数据源的使用条款。

## 示例结果

一次真实数据跑通后的示例指标如下：

- `total_return`: `2.2712`
- `annual_return`: `0.2731`
- `annual_volatility`: `0.2270`
- `sharpe`: `1.2031`
- `max_drawdown`: `-0.3218`

对应输出文件保存在 `outputs/reports/` 目录下。
