"""
事件层模块 v2.3
三个独立 Provider + 统一状态管理 + 标准输出结构

v2.3 新增：
  - build_events_list()：将三类事件统一为标准化 events_list
  - event_id 规则：source_type + anchor_code + trade_date + title_hash(8位)
  - 输出结构新增 events_list 字段，兼容现有 reporter 读取逻辑
"""

import time
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

DEFAULT_CONFIG_PATH = CONFIG_DIR / "stocks.yaml"
DEFAULT_EVENTS_OUTPUT = DATA_PROCESSED_DIR / "daily_events.json"

_REQUEST_TIMEOUT = 8    # 秒，单次请求超时
_REQUEST_INTERVAL = 1.0  # 秒，请求间隔


# ─────────────────────────────────────────────
# 配置加载
# ─────────────────────────────────────────────

def load_config(config_path: str = None) -> Dict[str, Any]:
    if config_path is None:
        config_path = DEFAULT_CONFIG_PATH
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


# ─────────────────────────────────────────────
# AnnouncementProvider
# 数据源：巨潮资讯全文搜索（best-effort，L1 官方源）
# 职责：获取铂力特当日公告标题、日期、PDF 链接
# ─────────────────────────────────────────────

class AnnouncementProvider:
    """
    公司公告 Provider（巨潮资讯，best-effort）

    接口：http://www.cninfo.com.cn/new/fulltextSearch/full
    参数：searchkey=铂力特, sdate/edate=trade_date
    返回：标题、日期、PDF 链接
    不做：正文抓取、正文摘要、情感判断

    v2.2.1 状态：best-effort，失败直接降级，不影响主流程
    """

    BASE_URL = "http://www.cninfo.com.cn/new/fulltextSearch/full"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "http://www.cninfo.com.cn",
    }
    PDF_BASE = "http://static.cninfo.com.cn/"

    def fetch(self, anchor_name: str, trade_date: str) -> Tuple[List[Dict], str]:
        """
        获取当日公告

        Args:
            anchor_name: 公司名称，如 铂力特（用于搜索）
            trade_date: 交易日期，格式 YYYYMMDD

        Returns:
            (announcements, status)
            announcements: list of {title, date, url}
            status: ok / empty / timeout / error
        """
        import requests

        date_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

        try:
            params = {
                "searchkey": anchor_name,
                "sdate": date_fmt,
                "edate": date_fmt,
                "isfulltext": False,
                "sortName": "time",
                "sortType": "desc",
                "pageNum": 1,
                "pageSize": 20,
            }
            resp = requests.get(
                self.BASE_URL, params=params,
                headers=self.HEADERS, timeout=_REQUEST_TIMEOUT
            )
            resp.raise_for_status()
            data = resp.json()

            raw_list = data.get("announcements") or []

            # 过滤：只保留 secCode 匹配的公告（避免同名公司干扰）
            results = []
            for item in raw_list:
                title_raw = item.get("announcementTitle", "")
                # 去掉巨潮的 <em> 高亮标签
                title = title_raw.replace("<em>", "").replace("</em>", "").strip()

                # 时间戳转日期（毫秒级）
                ts_ms = item.get("announcementTime")
                if ts_ms:
                    ann_date = datetime.fromtimestamp(int(ts_ms) / 1000).strftime("%Y-%m-%d")
                else:
                    ann_date = date_fmt

                # PDF 链接
                adjunct = item.get("adjunctUrl", "")
                url = f"{self.PDF_BASE}{adjunct}" if adjunct else ""

                results.append({
                    "title": title,
                    "date": ann_date,
                    "url": url,
                })

            status = "ok" if results else "empty"
            return results, status

        except requests.exceptions.Timeout:
            print(f"[WARN] AnnouncementProvider: 请求超时")
            return [], "timeout"
        except Exception as e:
            print(f"[WARN] AnnouncementProvider: {e}")
            return [], "error"


# ─────────────────────────────────────────────
# CompanyNewsProvider
# 数据源：东方财富 stock_news_em（L2 结构化聚合源）
# 职责：获取铂力特当日相关新闻
# ─────────────────────────────────────────────

class CompanyNewsProvider:
    """
    公司新闻 Provider（东方财富）

    接口：akshare.stock_news_em(symbol=code)
    过滤：按 trade_date 当日过滤
    限制：只返回最近10条，当天运行时效性足够
    不做：正文解析、情感分析
    """

    def fetch(self, anchor_code: str, trade_date: str, max_items: int = 3) -> Tuple[List[Dict], str]:
        """
        获取当日公司新闻

        Returns:
            (news_list, status)
        """
        import akshare as ak

        pure_code = anchor_code.split(".")[0]
        date_prefix = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

        try:
            time.sleep(_REQUEST_INTERVAL)
            df = ak.stock_news_em(symbol=pure_code)

            if df.empty:
                return [], "empty"

            df_today = df[df["发布时间"].astype(str).str.startswith(date_prefix)]

            if df_today.empty:
                return [], "empty"

            df_today = df_today.sort_values("发布时间", ascending=False).head(max_items)

            results = [
                {
                    "title": str(row.get("新闻标题", "")).strip(),
                    "datetime": str(row.get("发布时间", "")).strip(),
                    "source": str(row.get("文章来源", "")).strip(),
                }
                for _, row in df_today.iterrows()
                if str(row.get("新闻标题", "")).strip()
            ]

            return results, "ok"

        except Exception as e:
            print(f"[WARN] CompanyNewsProvider: {e}")
            return [], "error"


# ─────────────────────────────────────────────
# SectorNewsProvider
# 数据源：东方财富行业新闻（关键词检索，L2 结构化聚合源）
# 职责：按板块关键词检索当日行业相关新闻
# 策略：不依赖 core_universe 各股拼接，直接用关键词检索
# ─────────────────────────────────────────────

class SectorNewsProvider:
    """
    板块新闻 Provider（东方财富行业新闻，关键词检索）

    策略：
      直接用板块关键词检索东方财富行业新闻接口，
      不依赖 core_universe 各股新闻拼接。

    接口：akshare.stock_news_em 的行业/概念版本
    备用：对 core_universe 各股新闻做关键词过滤（降级）

    关键词分层：
      高精度词：商业航天、增材制造、金属增材、3D打印金属
      中精度词：航天发动机、运载火箭、卫星互联网、低轨卫星
      公司名词：航天电子、航天动力、航天工程
      政策词  ：航天强国、商业发射
    """

    def fetch(
        self,
        sector_keywords: List[str],
        core_codes: List[str],
        trade_date: str,
        max_items: int = 3
    ) -> Tuple[List[Dict], str]:
        """
        获取当日板块新闻

        策略：
        1. 优先：用关键词检索东方财富行业新闻
        2. 降级：对 core_universe 各股新闻做关键词过滤

        Returns:
            (news_list, status)
        """
        # 先尝试关键词直接检索
        results, status = self._fetch_by_keywords(sector_keywords, trade_date, max_items)
        if status in ("ok",) and results:
            return results, status

        # 降级：core_universe 各股新闻 + 关键词过滤
        print("[INFO] SectorNewsProvider: 关键词检索无结果，降级为 core_universe 过滤")
        return self._fetch_by_core_universe(sector_keywords, core_codes, trade_date, max_items)

    def _fetch_by_keywords(
        self,
        sector_keywords: List[str],
        trade_date: str,
        max_items: int
    ) -> Tuple[List[Dict], str]:
        """
        直接用关键词检索东方财富行业新闻

        使用 akshare.stock_news_em 的行业概念版本，
        或东方财富行业新闻接口（如可用）。

        去噪规则：
        - 命中关键词必须出现在标题中（已有）
        - 额外排除明显噪音：标题中含排除词时跳过
        """
        import akshare as ak

        date_prefix = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
        all_news = []
        seen_titles = set()

        # 对高精度关键词逐一检索（只用前4个高精度词，避免请求过多）
        high_precision_keywords = sector_keywords[:4]

        for kw in high_precision_keywords:
            try:
                time.sleep(_REQUEST_INTERVAL)
                df = ak.stock_news_em(symbol=kw)
                if df.empty:
                    continue

                df_today = df[df["发布时间"].astype(str).str.startswith(date_prefix)]
                if df_today.empty:
                    continue

                for _, row in df_today.iterrows():
                    title = str(row.get("新闻标题", "")).strip()
                    if not title or title in seen_titles:
                        continue

                    # 去噪：排除与航天/增材制造无关的噪音标题
                    if self._is_noise(title, kw):
                        continue

                    seen_titles.add(title)
                    all_news.append({
                        "title": title,
                        "datetime": str(row.get("发布时间", "")).strip(),
                        "source": str(row.get("文章来源", "")).strip(),
                        "matched_keyword": kw,
                    })
            except Exception as e:
                print(f"[WARN] SectorNewsProvider keyword={kw}: {e}")
                continue

        if not all_news:
            return [], "empty"

        all_news.sort(key=lambda x: x["datetime"], reverse=True)
        return all_news[:max_items], "ok"

    # 噪音排除词：标题中含这些词时，即使命中关键词也跳过
    _NOISE_EXCLUDE_TERMS = [
        # 消费品/玩具/版权类（3D打印噪音）
        "泡泡玛特", "手办", "玩具", "版权", "IP授权", "盲盒",
        # 医疗/牙科类（3D打印噪音）
        "牙科", "义齿", "骨科", "医疗器械", "种植牙",
        # 建筑/房地产类
        "建筑打印", "房屋打印", "混凝土打印",
        # 纯行情播报类（无实质内容）
        "概念下跌", "概念上涨", "主力资金净流出", "主力资金净流入",
        "融资余额", "排行榜", "龙虎榜",
    ]

    def _is_noise(self, title: str, matched_keyword: str) -> bool:
        """
        判断是否为噪音标题

        规则：
        1. 标题含排除词 → 噪音
        2. 关键词为"增材制造"/"3D打印金属"/"金属增材"时，
           标题中没有航天/工业/金属相关词 → 噪音
        """
        # 规则1：排除词命中
        for term in self._NOISE_EXCLUDE_TERMS:
            if term in title:
                return True

        # 规则2：增材制造类关键词的额外验证
        if matched_keyword in ("增材制造", "3D打印金属", "金属增材"):
            # 标题中必须含有工业/航天相关词才保留
            industrial_terms = [
                "航天", "航空", "工业", "金属", "钛合金", "高温合金",
                "铝合金", "不锈钢", "零件", "制造", "成形", "烧结",
            ]
            if not any(t in title for t in industrial_terms):
                return True

        return False

    def _fetch_by_core_universe(
        self,
        sector_keywords: List[str],
        core_codes: List[str],
        trade_date: str,
        max_items: int
    ) -> Tuple[List[Dict], str]:
        """
        降级方案：对 core_universe 各股新闻做关键词过滤
        """
        import akshare as ak

        date_prefix = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"
        all_news = []
        seen_titles = set()
        any_success = False

        for code in core_codes:
            try:
                time.sleep(_REQUEST_INTERVAL)
                df = ak.stock_news_em(symbol=code)
                if df.empty:
                    continue

                any_success = True
                df_today = df[df["发布时间"].astype(str).str.startswith(date_prefix)]
                if df_today.empty:
                    continue

                for _, row in df_today.iterrows():
                    title = str(row.get("新闻标题", "")).strip()
                    if not title or title in seen_titles:
                        continue

                    matched_kw = next(
                        (kw for kw in sector_keywords if kw in title), None
                    )
                    if matched_kw is None:
                        continue

                    seen_titles.add(title)
                    all_news.append({
                        "title": title,
                        "datetime": str(row.get("发布时间", "")).strip(),
                        "source": str(row.get("文章来源", "")).strip(),
                        "matched_keyword": matched_kw,
                    })

            except Exception as e:
                print(f"[WARN] SectorNewsProvider core_universe code={code}: {e}")
                continue

        if not any_success:
            return [], "error"

        if not all_news:
            return [], "empty"

        all_news.sort(key=lambda x: x["datetime"], reverse=True)
        return all_news[:max_items], "ok"


# ─────────────────────────────────────────────
# events_list 标准化构建（v2.3）
# ─────────────────────────────────────────────

def _make_event_id(source_type: str, anchor_code: str, trade_date: str, title: str) -> str:
    """
    生成稳定的事件唯一标识

    规则：source_type + anchor_code + trade_date + title_hash(8位)
    不做模糊去重，不做相似度匹配，只做精确 title 哈希。
    """
    import hashlib
    title_hash = hashlib.md5(title.encode("utf-8")).hexdigest()[:8]
    code_clean = anchor_code.replace(".", "_")
    return f"{source_type}__{code_clean}__{trade_date}__{title_hash}"


def build_events_list(
    trade_date: str,
    anchor_code: str,
    anchor_name: str,
    announcements: List[Dict],
    announcement_status: str,
    company_news: List[Dict],
    company_news_status: str,
    sector_news: List[Dict],
    sector_news_status: str,
    fetched_at: str,
) -> List[Dict]:
    """
    将三类事件统一为标准化 events_list

    每条事件结构：
      event_id        - 唯一标识（source_type + anchor_code + trade_date + title_hash）
      event_type      - announcement / company_news / sector_news
      source_type     - official（巨潮）/ aggregator（东方财富）/ fallback
      source_name     - cninfo / eastmoney / ...
      title           - 标题
      url             - 链接（可为空字符串）
      published_at    - 原始发布时间（字符串，保留原始格式）
      trade_date      - 关联交易日（YYYYMMDD）
      status          - 对应 Provider 的状态值
      anchor_code     - 关联标的代码
      anchor_name     - 关联标的名称
      keyword_hits    - 命中的关键词列表（sector_news 时有值，其余为空列表）
      fetched_at      - 抓取时间
    """
    events = []

    # ── 公告 ──
    for item in announcements:
        title = item.get("title", "").strip()
        if not title:
            continue
        events.append({
            "event_id":     _make_event_id("official", anchor_code, trade_date, title),
            "event_type":   "announcement",
            "source_type":  "official",
            "source_name":  "cninfo",
            "title":        title,
            "url":          item.get("url", ""),
            "published_at": item.get("date", ""),
            "trade_date":   trade_date,
            "status":       announcement_status,
            "anchor_code":  anchor_code,
            "anchor_name":  anchor_name,
            "keyword_hits": [],
            "fetched_at":   fetched_at,
        })

    # ── 公司新闻 ──
    for item in company_news:
        title = item.get("title", "").strip()
        if not title:
            continue
        events.append({
            "event_id":     _make_event_id("aggregator", anchor_code, trade_date, title),
            "event_type":   "company_news",
            "source_type":  "aggregator",
            "source_name":  "eastmoney",
            "title":        title,
            "url":          item.get("url", ""),
            "published_at": item.get("datetime", ""),
            "trade_date":   trade_date,
            "status":       company_news_status,
            "anchor_code":  anchor_code,
            "anchor_name":  anchor_name,
            "keyword_hits": [],
            "fetched_at":   fetched_at,
        })

    # ── 板块新闻 ──
    for item in sector_news:
        title = item.get("title", "").strip()
        if not title:
            continue
        events.append({
            "event_id":     _make_event_id("aggregator", anchor_code, trade_date, title),
            "event_type":   "sector_news",
            "source_type":  "aggregator",
            "source_name":  "eastmoney",
            "title":        title,
            "url":          item.get("url", ""),
            "published_at": item.get("datetime", ""),
            "trade_date":   trade_date,
            "status":       sector_news_status,
            "anchor_code":  anchor_code,
            "anchor_name":  anchor_name,
            "keyword_hits": item.get("matched_keyword", []) if isinstance(item.get("matched_keyword"), list)
                            else ([item["matched_keyword"]] if item.get("matched_keyword") else []),
            "fetched_at":   fetched_at,
        })

    return events


# ─────────────────────────────────────────────
# 信号判断（固化规则）
# ─────────────────────────────────────────────

def compute_event_signal_label(
    announcements: List[Dict],
    company_news: List[Dict],
    sector_news: List[Dict],
    announcement_status: str
) -> str:
    """
    event_signal_label 规则（v2.2.1，固化）：

    注意：公告在 announcement_status=ok 时才纳入判断，
          unavailable/timeout/error 时不影响信号。

    有明确催化：
      - announcement_status=ok 且公告 >= 1 条
      - 或公司新闻 >= 2 条
    有弱催化：
      - 公司新闻 = 1 条
      - 或板块新闻 >= 1 条
    无明确催化：
      - 公司新闻和板块新闻均为空
    """
    n_ann = len(announcements) if announcement_status == "ok" else 0
    n_co = len(company_news)
    n_sec = len(sector_news)

    if n_ann >= 1 or n_co >= 2:
        return "有明确催化"
    if n_co == 1 or n_sec >= 1:
        return "有弱催化"
    return "无明确催化"


def build_event_summary(
    trade_date: str,
    anchor_name: str,
    announcements: List[Dict],
    announcement_status: str,
    company_news: List[Dict],
    company_news_status: str,
    sector_news: List[Dict],
    sector_news_status: str,
    signal_label: str
) -> str:
    """
    规则化摘要（保守表达，不做推断）

    格式：
    "{date}：{公告描述}，{公司新闻描述}，{板块新闻描述}；初步判断：{signal_label}"

    关键原则：
    - status 非 ok/empty 时，不写"无"，写"未获取到"
    - 只描述事实，不推断原因
    """
    parts = []

    # 公告描述
    if announcement_status == "ok":
        n = len(announcements)
        parts.append(f"{n} 条{anchor_name}公告" if n > 0 else f"无{anchor_name}公告")
    elif announcement_status == "empty":
        parts.append(f"无{anchor_name}公告")
    else:
        parts.append(f"公告未获取（{announcement_status}）")

    # 公司新闻描述
    if company_news_status in ("ok", "empty"):
        n = len(company_news)
        parts.append(f"{n} 条公司新闻" if n > 0 else "无公司新闻")
    else:
        parts.append(f"公司新闻未获取（{company_news_status}）")

    # 板块新闻描述
    if sector_news_status in ("ok", "empty"):
        n = len(sector_news)
        parts.append(f"{n} 条板块新闻" if n > 0 else "无板块新闻")
    else:
        parts.append(f"板块新闻未获取（{sector_news_status}）")

    return f"{trade_date}：{'，'.join(parts)}；初步判断：{signal_label}"


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────

def collect_events(
    trade_date: str,
    anchor_code: str,
    anchor_name: str,
    config_path: str = None,
    output_path: str = None
) -> Dict[str, Any]:
    """
    收集当日事件，返回标准输出结构

    各 Provider 独立运行，任一失败不影响其他。
    """
    import json

    if output_path is None:
        output_path = DEFAULT_EVENTS_OUTPUT

    # 加载配置
    try:
        config = load_config(config_path)
    except Exception as e:
        result = _empty_result(trade_date, anchor_code, anchor_name, str(e))
        _save_events(result, output_path)
        return result

    core_universe = config.get("core_universe", [])
    core_codes = [s["code"].split(".")[0] for s in core_universe]

    event_cfg = config.get("event_keywords", {})
    sector_keywords = event_cfg.get("sector", _default_sector_keywords())

    print(f"[INFO] 事件层 v2.2.1：获取 {trade_date} 的事件数据...")

    # ── Provider 1: 公告 ──
    print("[INFO] AnnouncementProvider: 获取公司公告...")
    ann_provider = AnnouncementProvider()
    announcements, announcement_status = ann_provider.fetch(anchor_name, trade_date)
    print(f"[INFO] 公告: status={announcement_status}, 条数={len(announcements)}")

    # ── Provider 2: 公司新闻 ──
    print("[INFO] CompanyNewsProvider: 获取公司新闻...")
    co_provider = CompanyNewsProvider()
    company_news, company_news_status = co_provider.fetch(anchor_code, trade_date)
    print(f"[INFO] 公司新闻: status={company_news_status}, 条数={len(company_news)}")

    # ── Provider 3: 板块新闻 ──
    print("[INFO] SectorNewsProvider: 获取板块新闻...")
    sec_provider = SectorNewsProvider()
    sector_news, sector_news_status = sec_provider.fetch(
        sector_keywords, core_codes, trade_date
    )
    print(f"[INFO] 板块新闻: status={sector_news_status}, 条数={len(sector_news)}")

    # ── 信号判断 ──
    signal_label = compute_event_signal_label(
        announcements, company_news, sector_news, announcement_status
    )

    # ── 摘要 ──
    event_summary = build_event_summary(
        trade_date, anchor_name,
        announcements, announcement_status,
        company_news, company_news_status,
        sector_news, sector_news_status,
        signal_label
    )

    # ── events_list（v2.3 标准化结构）──
    fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    events_list = build_events_list(
        trade_date, anchor_code, anchor_name,
        announcements, announcement_status,
        company_news, company_news_status,
        sector_news, sector_news_status,
        fetched_at,
    )

    result = {
        "trade_date": trade_date,
        "anchor_code": anchor_code,
        "anchor_name": anchor_name,
        # 公告层
        "company_announcements": announcements,
        "announcement_status": announcement_status,
        # 公司新闻层
        "company_news": company_news,
        "company_news_status": company_news_status,
        # 板块新闻层
        "sector_news": sector_news,
        "sector_news_status": sector_news_status,
        # 综合判断
        "event_signal_label": signal_label,
        "event_summary": event_summary,
        # v2.3 标准化事件列表（reporter 现阶段不读此字段，为后续数据化准备）
        "events_list": events_list,
        # 元信息
        "error": None,
        "generated_at": fetched_at,
    }

    _save_events(result, output_path)
    print(f"[INFO] 事件数据已保存至: {output_path}")
    print(f"[INFO] 事件摘要: {event_summary}")

    return result


def _default_sector_keywords() -> List[str]:
    """
    默认板块关键词（聚焦商业航天主线）

    分层：
      高精度（前4个）：直接用于关键词检索
      中精度：用于 core_universe 降级过滤
      公司名/政策词：用于 core_universe 降级过滤
    """
    return [
        # 高精度词（直接检索）
        "商业航天", "增材制造", "金属增材", "3D打印金属",
        # 中精度词
        "航天发动机", "运载火箭", "卫星互联网", "低轨卫星",
        # 公司名词
        "航天电子", "航天动力", "航天工程",
        # 政策词
        "航天强国", "商业发射",
    ]


def _empty_result(
    trade_date: str,
    anchor_code: str,
    anchor_name: str,
    error: str
) -> Dict[str, Any]:
    return {
        "trade_date": trade_date,
        "anchor_code": anchor_code,
        "anchor_name": anchor_name,
        "company_announcements": [],
        "announcement_status": "error",
        "company_news": [],
        "company_news_status": "error",
        "sector_news": [],
        "sector_news_status": "error",
        "event_signal_label": "无明确催化",
        "event_summary": f"{trade_date}：事件层初始化失败（{error}）",
        "events_list": [],
        "error": error,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _save_events(result: Dict[str, Any], output_path) -> None:
    import json
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


# ─────────────────────────────────────────────
# 便捷加载（供 reporter 使用）
# ─────────────────────────────────────────────

def load_events(events_path: str = None) -> Optional[Dict[str, Any]]:
    """加载已保存的事件数据，不存在时返回 None"""
    import json
    if events_path is None:
        events_path = DEFAULT_EVENTS_OUTPUT

    events_file = Path(events_path)
    if not events_file.exists():
        return None

    try:
        with open(events_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] 加载事件数据失败: {e}")
        return None


def main():
    """独立运行事件层（调试用）"""
    import pandas as pd

    metrics_path = DATA_PROCESSED_DIR / "daily_metrics.parquet"
    if not metrics_path.exists():
        print("[ERROR] 请先运行 analyzer 生成 daily_metrics.parquet")
        return

    df = pd.read_parquet(metrics_path)
    latest = df.sort_values("trade_date", ascending=False).iloc[0]
    trade_date = pd.Timestamp(latest["trade_date"]).strftime("%Y%m%d")
    anchor_code = latest["anchor_symbol"]

    config = load_config()
    anchor_name = config["anchor"]["name"]

    print(f"交易日期: {trade_date}, 标的: {anchor_name}（{anchor_code}）")
    result = collect_events(trade_date, anchor_code, anchor_name)

    print("\n=== 事件层结果 ===")
    print(f"信号: {result['event_signal_label']}")
    print(f"摘要: {result['event_summary']}")
    print(f"公告: {len(result['company_announcements'])} 条 [{result['announcement_status']}]")
    print(f"公司新闻: {len(result['company_news'])} 条 [{result['company_news_status']}]")
    print(f"板块新闻: {len(result['sector_news'])} 条 [{result['sector_news_status']}]")


if __name__ == "__main__":
    main()
