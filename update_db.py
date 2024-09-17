import argparse
import asyncio
import datetime
import logging
import os
from typing import Any, Dict, List, Tuple

from aiohttp_client_cache.backends.sqlite import SQLiteBackend
from aiohttp_client_cache.session import CachedSession
from dotenv import load_dotenv
from fake_useragent import UserAgent
from tortoise import Tortoise, run_async

from log_helper import setup_logging
from models import HistoryTrade, Stock
from utils import get_today

load_dotenv()

LOGGER_ = logging.getLogger("update_db")

parser = argparse.ArgumentParser()
parser.add_argument("--date", default="", help="Date to crawl")
parser.add_argument("--test", action="store_true", help="Test mode", default=False)
args = parser.parse_args()

ua = UserAgent()


async def crawl_stocks(session: CachedSession) -> List[Tuple[str, bool]]:
    """
    取得上市公司代號與上櫃公司代號
    """
    result: List[Tuple[str, bool]] = []

    async with session.get(
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
        headers={"User-Agent": ua.random},
    ) as resp:  # 取得所有上市公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["公司代號"].isdigit():
                stock_id = d["公司代號"]
                result.append((stock_id, True))
                await Stock.silent_create(id=stock_id, name=d["公司簡稱"])

    async with session.get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        headers={"User-Agent": ua.random},
        proxy=os.getenv("PROXY"),
    ) as resp:  # 取得所有上櫃公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["SecuritiesCompanyCode"].isdigit():
                stock_id = d["SecuritiesCompanyCode"]
                result.append((stock_id, False))
                await Stock.silent_create(id=stock_id, name=d["CompanyName"])

    LOGGER_.info("Finished crawling stocks, total: %d", len(result))
    return result


async def crawl_twse_history_trades(
    stock_id: str, date: str, session: CachedSession
) -> int:
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_id}"

        created = 0
        async with session.get(url, headers={"User-Agent": ua.random}) as resp:
            data: Dict[str, Any] = await resp.json()
            if data["total"] == 0:
                return 0

            for d in data["data"]:
                history_trade = HistoryTrade.parse_twse(d, stock_id)
                saved = await history_trade.silent_save()
                created += 1 if saved else 0

            LOGGER_.info("Created %d history trades for %s", created, stock_id)
    except Exception:
        LOGGER_.exception(
            "Error occurred while crawling twse history trades for %s", stock_id
        )
        return 0
    else:
        return created


async def crawl_tpex_history_trades(
    stock_id: str, date: str, session: CachedSession
) -> int:
    try:
        created = 0
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={date}&stkno={stock_id}"
        async with session.get(url, headers={"User-Agent": ua.random}, proxy=os.getenv("PROXY")) as resp:
            data = await resp.json(content_type="text/html")
            if data["iTotalRecords"] == 0:
                return 0

            for d in data["aaData"]:
                history_trade = HistoryTrade.parse_tpex(d, stock_id)
                saved = await history_trade.silent_save()
                created += 1 if saved else 0

            LOGGER_.info("Created %d history trades for %s", created, stock_id)
    except Exception:
        LOGGER_.exception(
            "Error occurred while crawling tpex history trades for %s", stock_id
        )
        return 0
    else:
        return created


async def main() -> None:
    await Tortoise.init(
        db_url=os.getenv("DB_URL") or "sqlite://db.sqlite3",
        modules={"models": ["models"]},
    )
    await Tortoise.generate_schemas()

    if args.test:
        today = datetime.datetime(2024, 2, 16)
    else:
        today = (
            datetime.datetime.strptime(args.date, "%Y%m%d")
            if args.date
            else get_today()
        )

    if today.weekday() in {5, 6}:
        LOGGER_.info("Today is not a trading day")
        return

    twse_date = today.strftime("%Y%m%d")
    tpex_date = f"{today.year - 1911}/{today.month}"
    total = 0

    async with CachedSession(cache=SQLiteBackend(expire_after=60 * 60)) as session:
        if args.test:
            stock_id_tuples = [("2330", True), ("6417", False)]
        else:
            stock_id_tuples = await crawl_stocks(session)

        for stock_id_tuple in stock_id_tuples:
            stock_id, is_twse = stock_id_tuple

            if len(stock_id) != 4:
                continue

            if is_twse:
                created = await crawl_twse_history_trades(stock_id, twse_date, session)
            else:
                created = await crawl_tpex_history_trades(stock_id, tpex_date, session)
            total += created

            await asyncio.sleep(0.5)

    LOGGER_.info("Total created: %d", total)


with setup_logging():
    run_async(main())
