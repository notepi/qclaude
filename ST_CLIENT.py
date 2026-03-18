# -*- coding: utf-8 -*-
"""
ST_CLIENT.py - Tushare 代理接口封装
代理地址：https://tushare.citydata.club
接口规范与 Tushare 官方完全一致，只是通过代理服务调用。

使用方式：
    from ST_CLIENT import StockToday
    st = StockToday()
    df = st.daily(ts_code='688333.SH', start_date='20260310', end_date='20260317')
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://tushare.citydata.club"


class StockToday:
    def __init__(self, token: str = None):
        self.url = BASE_URL
        self.TOKEN = token or os.getenv("CITYDATA_TOKEN")
        if not self.TOKEN:
            raise ValueError("未找到 CITYDATA_TOKEN，请在 .env 中填入")

    def _post(self, endpoint: str, params: dict) -> list:
        """通用 POST 请求，返回 list of dict"""
        params["TOKEN"] = self.TOKEN
        url = f"{self.url}/{endpoint}"
        resp = requests.post(url, data=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "data" in data:
            # 错误响应：{"data": "TOKEN失效，请联系TAOBAO"}
            raise ValueError(f"接口返回错误: {data['data']}")
        return data  # list of dict

    def _to_df(self, endpoint: str, params: dict) -> pd.DataFrame:
        """调用接口并返回 DataFrame"""
        data = self._post(endpoint, params)
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)

    # ─────────────────────────────────────────────
    # TOKEN 状态查询
    # ─────────────────────────────────────────────

    def token_status(self) -> dict:
        """查询 TOKEN 有效期"""
        url = f"{self.url}/TOKEN"
        resp = requests.post(url, data={"TOKEN": self.TOKEN}, timeout=10)
        return resp.json()

    # ─────────────────────────────────────────────
    # 股票基础数据
    # ─────────────────────────────────────────────

    def stock_basic(self, exchange: str = "", list_status: str = "L",
                    fields: str = "ts_code,symbol,name,area,industry,list_date") -> pd.DataFrame:
        """股票基础信息（对应 tushare: stock_basic）"""
        return self._to_df("stock_basic", {
            "exchange": exchange,
            "list_status": list_status,
            "fields": fields,
        })

    # ─────────────────────────────────────────────
    # 日线行情
    # ─────────────────────────────────────────────

    def daily(self, ts_code: str = "", trade_date: str = "",
              start_date: str = "", end_date: str = "",
              fields: str = "") -> pd.DataFrame:
        """A股日线行情（对应 tushare: daily）
        字段：ts_code, trade_date, open, high, low, close, pre_close,
              change, pct_chg, vol, amount
        amount 单位：千元（Tushare 原始）
        """
        params = {}
        if ts_code:    params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date:   params["end_date"] = end_date
        if fields:     params["fields"] = fields
        return self._to_df("daily", params)

    # ─────────────────────────────────────────────
    # 指数日线
    # ─────────────────────────────────────────────

    def index_daily(self, ts_code: str, start_date: str = "",
                    end_date: str = "", trade_date: str = "") -> pd.DataFrame:
        """指数日线行情（对应 tushare: index_daily）"""
        params = {"ts_code": ts_code}
        if start_date: params["start_date"] = start_date
        if end_date:   params["end_date"] = end_date
        if trade_date: params["trade_date"] = trade_date
        return self._to_df("index_daily", params)

    # ─────────────────────────────────────────────
    # 复权行情
    # ─────────────────────────────────────────────

    def pro_bar(self, ts_code: str, start_date: str = "", end_date: str = "",
                adj: str = "qfq", freq: str = "D") -> pd.DataFrame:
        """复权行情（对应 tushare: pro_bar）"""
        return self._to_df("pro_bar", {
            "ts_code": ts_code,
            "adj": adj,
            "freq": freq,
            "start_date": start_date,
            "end_date": end_date,
        })

    # ─────────────────────────────────────────────
    # 财务数据
    # ─────────────────────────────────────────────

    def income(self, ts_code: str, period: str = "", start_date: str = "",
               end_date: str = "", fields: str = "") -> pd.DataFrame:
        """利润表（对应 tushare: income）"""
        params = {"ts_code": ts_code}
        if period:     params["period"] = period
        if start_date: params["start_date"] = start_date
        if end_date:   params["end_date"] = end_date
        if fields:     params["fields"] = fields
        return self._to_df("income", params)

    def balancesheet(self, ts_code: str, period: str = "", fields: str = "") -> pd.DataFrame:
        """资产负债表（对应 tushare: balancesheet）"""
        params = {"ts_code": ts_code}
        if period: params["period"] = period
        if fields: params["fields"] = fields
        return self._to_df("balancesheet", params)

    def cashflow(self, ts_code: str, period: str = "", fields: str = "") -> pd.DataFrame:
        """现金流量表（对应 tushare: cashflow）"""
        params = {"ts_code": ts_code}
        if period: params["period"] = period
        if fields: params["fields"] = fields
        return self._to_df("cashflow", params)

    def fina_indicator(self, ts_code: str, period: str = "", fields: str = "") -> pd.DataFrame:
        """财务指标（对应 tushare: fina_indicator）"""
        params = {"ts_code": ts_code}
        if period: params["period"] = period
        if fields: params["fields"] = fields
        return self._to_df("fina_indicator", params)

    # ─────────────────────────────────────────────
    # 公告 / 新闻
    # ─────────────────────────────────────────────

    def anns_d(self, ts_code: str = "", trade_date: str = "",
               start_date: str = "", end_date: str = "") -> pd.DataFrame:
        """每日公告（对应 tushare: anns_d）"""
        params = {}
        if ts_code:    params["ts_code"] = ts_code
        if trade_date: params["trade_date"] = trade_date
        if start_date: params["start_date"] = start_date
        if end_date:   params["end_date"] = end_date
        return self._to_df("anns_d", params)

    def news(self, src: str = "sina", start_date: str = "",
             end_date: str = "", fields: str = "") -> pd.DataFrame:
        """新闻快讯（对应 tushare: news）"""
        params = {"src": src}
        if start_date: params["start_date"] = start_date
        if end_date:   params["end_date"] = end_date
        if fields:     params["fields"] = fields
        return self._to_df("news", params)
