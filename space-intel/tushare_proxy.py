"""
tushare_proxy.py - Tushare 透明代理层

目标：业务代码零改动，继续按原生 tushare 写法调用。
底层自动走 citydata.club 代理，不走官方 Tushare SDK。

用法（替换原来的 tushare 初始化）：
    # 原来：
    import tushare as ts
    pro = ts.pro_api(token)

    # 现在：
    from tushare_proxy import pro_api
    pro = pro_api(token)

    # 之后完全一样：
    df = pro.daily(ts_code='688333.SH', start_date='20260310', end_date='20260317')
    df = pro.income(ts_code='688333.SH')
"""

import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PROXY_BASE_URL = "https://tushare.citydata.club"


class TushareProxyAPI:
    """
    模拟 tushare.pro_api 对象。
    所有方法签名与 tushare 官方完全一致，底层走代理。
    """

    def __init__(self, token: str = None):
        self._token = (
            token
            or os.getenv("CITYDATA_TOKEN")
            or os.getenv("TUSHARE_TOKEN")
        )
        if not self._token:
            raise ValueError(
                "未找到 token，请在 .env 中设置 CITYDATA_TOKEN"
            )
        self._base_url = PROXY_BASE_URL

    # ─────────────────────────────────────────────
    # 核心调用层（所有接口共用）
    # ─────────────────────────────────────────────

    def _call(self, api_name: str, **kwargs) -> pd.DataFrame:
        """
        通用代理调用。
        将 kwargs 转为 POST 表单，返回 DataFrame。
        """
        params = {"TOKEN": self._token}
        for k, v in kwargs.items():
            if v is not None and v != "":
                params[k] = v

        url = f"{self._base_url}/{api_name}"
        try:
            resp = requests.post(url, data=params, timeout=15)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 404:
                raise NotImplementedError(
                    f"代理服务不支持接口: {api_name}（404）"
                )
            raise

        data = resp.json()

        # 错误响应检测
        if isinstance(data, dict) and "data" in data:
            msg = str(data["data"])
            if "TOKEN" in msg or "失效" in msg or "token" in msg.lower():
                raise PermissionError(f"Token 无效或已过期: {msg}")

        if not data:
            return pd.DataFrame()

        return pd.DataFrame(data)

    # ─────────────────────────────────────────────
    # __getattr__：未显式定义的接口自动代理
    # 这是核心魔法：pro.任意接口名() 都能工作
    # ─────────────────────────────────────────────

    def __getattr__(self, api_name: str):
        """
        动态代理任意 tushare 接口。
        pro.daily() / pro.income() / pro.balancesheet() ...
        全部自动路由到代理服务，无需逐一定义。
        """
        def _method(**kwargs):
            return self._call(api_name, **kwargs)
        _method.__name__ = api_name
        return _method

    # ─────────────────────────────────────────────
    # 显式定义高频接口（提供 IDE 补全 + 参数提示）
    # 功能上与 __getattr__ 等价，只是更友好
    # ─────────────────────────────────────────────

    def daily(self, ts_code: str = "", trade_date: str = "",
              start_date: str = "", end_date: str = "",
              fields: str = "") -> pd.DataFrame:
        """A股日线行情（amount 单位：千元）"""
        return self._call("daily", ts_code=ts_code, trade_date=trade_date,
                          start_date=start_date, end_date=end_date, fields=fields)

    def stock_basic(self, exchange: str = "", list_status: str = "L",
                    ts_code: str = "", fields: str = "") -> pd.DataFrame:
        """股票基础信息"""
        return self._call("stock_basic", exchange=exchange,
                          list_status=list_status, ts_code=ts_code, fields=fields)

    def index_daily(self, ts_code: str, start_date: str = "",
                    end_date: str = "", trade_date: str = "") -> pd.DataFrame:
        """指数日线行情"""
        return self._call("index_daily", ts_code=ts_code,
                          start_date=start_date, end_date=end_date,
                          trade_date=trade_date)

    def pro_bar(self, ts_code: str, start_date: str = "", end_date: str = "",
                adj: str = "qfq", freq: str = "D") -> pd.DataFrame:
        """复权行情"""
        return self._call("pro_bar", ts_code=ts_code, adj=adj, freq=freq,
                          start_date=start_date, end_date=end_date)

    def income(self, ts_code: str, period: str = "", start_date: str = "",
               end_date: str = "", fields: str = "") -> pd.DataFrame:
        """利润表"""
        return self._call("income", ts_code=ts_code, period=period,
                          start_date=start_date, end_date=end_date, fields=fields)

    def balancesheet(self, ts_code: str, period: str = "",
                     fields: str = "") -> pd.DataFrame:
        """资产负债表"""
        return self._call("balancesheet", ts_code=ts_code,
                          period=period, fields=fields)

    def cashflow(self, ts_code: str, period: str = "",
                 fields: str = "") -> pd.DataFrame:
        """现金流量表"""
        return self._call("cashflow", ts_code=ts_code,
                          period=period, fields=fields)

    def fina_indicator(self, ts_code: str, period: str = "",
                       fields: str = "") -> pd.DataFrame:
        """财务指标"""
        return self._call("fina_indicator", ts_code=ts_code,
                          period=period, fields=fields)


# ─────────────────────────────────────────────
# 对外接口：模拟 tushare.pro_api()
# ─────────────────────────────────────────────

def pro_api(token: str = None) -> TushareProxyAPI:
    """
    替代 tushare.pro_api()，返回代理 API 对象。

    用法：
        from tushare_proxy import pro_api
        pro = pro_api()          # 从 .env 读 CITYDATA_TOKEN
        pro = pro_api("your_token")  # 显式传入
    """
    return TushareProxyAPI(token=token)
