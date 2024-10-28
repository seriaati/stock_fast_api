import argparse
import asyncio
import datetime
import logging
import os
from typing import Any, Dict, List, Tuple

import aiohttp
from bs4 import BeautifulSoup
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
                await Stock.silent_create(id=stock_id, name=d["公司簡稱"])

    async with session.get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        headers={"User-Agent": ua.random},
    ) as resp:  # 取得所有上櫃公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["SecuritiesCompanyCode"].isdigit():
                stock_id = d["SecuritiesCompanyCode"]
                result.append((stock_id, False))
                await Stock.silent_create(id=stock_id, name=d["CompanyName"])

    LOGGER_.info("Finished crawling stock IDs, total: %d", len(result))
    return result


async def crawl_twse_history_trades(
    stock_id: str, date: str, session: aiohttp.ClientSession
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
    session: aiohttp.ClientSession, *, date: datetime.date
) -> int:
    try:
        url = f"https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?date={date.year}%2F{date.month}%2F{date.day}&type=EW&id=&response=html&order=0&sort=asc"
        async with aiohttp.ClientSession() as session, session.get(url) as resp:
            html = await resp.text()
    except Exception:
        LOGGER_.exception("Error occurred while crawling tpex history trades")
        return 0

    created = 0
    soup = BeautifulSoup(html, "lxml")
    tbody = soup.find("table").find("tbody")  # pyright: ignore[reportOptionalMemberAccess]

    for tr in tbody.find_all("tr"):  # pyright: ignore[reportOptionalMemberAccess, reportAttributeAccessIssue]
        try:
            trade = HistoryTrade.parse_tpex(
                [td.text for td in tr.find_all("td")], datetime.date(2024, 10, 28)
            )
        except Exception:
            LOGGER_.exception("Error occurred while parsing tpex history trades")
            continue

        if len(trade.stock_id) != 4:
            continue

        saved = await trade.silent_save()
        created += 1 if saved else 0
        LOGGER_.info(
            "Created %d history trades for %s", 1 if saved else 0, trade.stock_id
        )

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
    total = 0

    async with aiohttp.ClientSession() as session:
        if args.test:
            stock_ids = [("2330", True), ("6417", False)]
        else:
            stock_ids = await crawl_stocks(session)

        total += await crawl_tpex_history_trades(session, date=today)

        for stock_id, is_twse in stock_ids:
            if len(stock_id) != 4 or not is_twse:
                continue

            created = await crawl_twse_history_trades(stock_id, twse_date, session)
            total += created
            await asyncio.sleep(0.5)

    LOGGER_.info("Total created: %d", total)


with setup_logging():
    run_async(main())
