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
from utils import get_today, ignore_conflict_create

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
                await ignore_conflict_create(Stock(id=stock_id, name=d["公司簡稱"]))

    async with session.get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        headers={"User-Agent": ua.random},
    ) as resp:  # 取得所有上櫃公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["SecuritiesCompanyCode"].isdigit():
                stock_id = d["SecuritiesCompanyCode"]
                result.append((stock_id, False))
                await ignore_conflict_create(Stock(id=stock_id, name=d["CompanyName"]))

    return result


async def crawl_twse_history_trades(
    stock_id: str, date: str, session: aiohttp.ClientSession
) -> None:
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_id}"
        async with session.get(url, headers={"User-Agent": ua.random}) as resp:
            data: Dict[str, Any] = await resp.json()
            if data["total"] == 0:
                return
            history_trades = [
                HistoryTrade.parse_twse(d, stock_id) for d in data["data"]
            ]
            for trade in history_trades:
                await ignore_conflict_create(trade)
    except Exception as e:
        LOGGER_.error(
            f"An error occurred while crawling twse history trades for {stock_id} on date {date}",
            exc_info=e,
        )


async def crawl_tpex_history_trades(
    stock_id: str, date: str, session: aiohttp.ClientSession
) -> None:
    try:
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={date}&stkno={stock_id}"
        async with session.get(url, headers={"User-Agent": ua.random}) as resp:
            data = await resp.json(content_type="text/html")
            if data["iTotalRecords"] == 0:
                return
            history_trades = [
                HistoryTrade.parse_twse(d, stock_id) for d in data["aaData"]
            ]
            for trade in history_trades:
                await ignore_conflict_create(trade)
    except Exception as e:
        LOGGER_.error(
            f"An error occurred while crawling tpex history trades for {stock_id} on date {date}",
            exc_info=e,
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
                LOGGER_.info(f"Start crawling {stock_id} on date {twse_date}")
                await crawl_twse_history_trades(stock_id, twse_date, session)
            else:
                LOGGER_.info(f"Start crawling {stock_id} on date {tpex_date}")
                await crawl_tpex_history_trades(stock_id, tpex_date, session)

            for month in range(1, today.month + 1):
                LOGGER_.info(f"Start crawling {stock_id} on {today.year}/{month}")
                month_str = str(month).zfill(2)
                if is_twse:
                    await crawl_twse_history_trades(
                        stock_id, f"{today.year}{month_str}01", session
                    )
                else:
                    await crawl_tpex_history_trades(
                        stock_id, f"{today.year - 1911}/{month_str}", session
                    )

            await asyncio.sleep(0.5)


with setup_logging():
    run_async(main())
