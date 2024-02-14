import argparse
import asyncio
import datetime
import logging
import os
from typing import Any, Dict, List, Tuple

import aiohttp
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
args = parser.parse_args()

ua = UserAgent()


async def crawl_stocks(session: aiohttp.ClientSession) -> List[Tuple[str, bool]]:
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
                await Stock(id=stock_id, name=d["公司簡稱"]).silent_create()

    async with session.get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        headers={"User-Agent": ua.random},
    ) as resp:  # 取得所有上櫃公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["SecuritiesCompanyCode"].isdigit():
                stock_id = d["SecuritiesCompanyCode"]
                result.append((stock_id, False))
                await Stock(id=stock_id, name=d["CompanyName"]).silent_create()

    LOGGER_.info("Finished crawling stocks, total: %d", len(result))
    return result


async def crawl_twse_history_trades(
    stock_id: str, date: str, session: aiohttp.ClientSession
) -> None:
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_id}"
        async with session.get(url, headers={"User-Agent": ua.random}) as resp:
            data: Dict[str, Any] = await resp.json()
            if data["total"] == 0:

            for d in data["data"]:
                history_trade = await HistoryTrade.parse_twse(
                    d, stock_id
                ).silent_create()
                if history_trade is not None:
                    created += 1
    except Exception:
        LOGGER_.exception(
            "Error occurred while crawling twse history trades for %s on date %s",
            stock_id,
            date,
        )


async def crawl_tpex_history_trades(
    stock_id: str, date: str, session: aiohttp.ClientSession
) -> None:
    try:
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={date}&stkno={stock_id}"
        async with session.get(url, headers={"User-Agent": ua.random}) as resp:
            data = await resp.json(content_type="text/html")
            if data["iTotalRecords"] == 0:

            for d in data["aaData"]:
                history_trade = await HistoryTrade.parse_tpex(
                    d, stock_id
                ).silent_create()
                if history_trade is not None:
                    created += 1
    except Exception:
        LOGGER_.exception(
            "Error occurred while crawling tpex history trades for %s on date %s",
            stock_id,
            date,
        )


async def main() -> None:
    await Tortoise.init(
        db_url=os.getenv("DB_URL") or "sqlite://db.sqlite3",
        modules={"models": ["models"]},
    )
    await Tortoise.generate_schemas()

    today = (
        datetime.datetime.strptime(args.date, "%Y%m%d") if args.date else get_today()
    )
    if today.weekday() in {5, 6}:
        LOGGER_.info("Today is not a trading day")
        return

    twse_date = today.strftime("%Y%m%d")
    tpex_date = f"{today.year - 1911}/{today.month}"

    async with aiohttp.ClientSession() as session:
        stock_id_tuples = await crawl_stocks(session)

        for stock_id_tuple in stock_id_tuples:
            stock_id, is_twse = stock_id_tuple

            if len(stock_id) != 4:
                continue

            if is_twse:
                await crawl_twse_history_trades(stock_id, twse_date, session)
            else:
                await crawl_tpex_history_trades(stock_id, tpex_date, session)

            await asyncio.sleep(0.5)


with setup_logging():
    run_async(main())
