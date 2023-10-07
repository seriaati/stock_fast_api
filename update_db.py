import argparse
import asyncio
import logging
import os
from typing import Any, Dict, List

import aiohttp
from dotenv import load_dotenv
from fake_useragent import UserAgent
from tortoise import Tortoise, run_async

from models import HistoryTrade, Stock
from utils import get_today, ignore_conflict_create

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

parser = argparse.ArgumentParser()
parser.add_argument("--year", type=int, default=0)
parser.add_argument("--id", type=str, default="")
args = parser.parse_args()

ua = UserAgent()


async def crawl_stocks(session: aiohttp.ClientSession) -> None:
    """
    取得上市公司代號與上櫃公司代號
    """
    async with session.get(
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
        headers={"User-Agent": ua.random},
    ) as resp:  # 取得所有上市公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["公司代號"].isdigit():
                stock_id = d["公司代號"]
                await ignore_conflict_create(Stock(id=stock_id, name=d["公司簡稱"]))

    async with session.get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        headers={"User-Agent": ua.random},
    ) as resp:  # 取得所有上櫃公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["SecuritiesCompanyCode"].isdigit():
                stock_id = d["SecuritiesCompanyCode"]
                await ignore_conflict_create(Stock(id=stock_id, name=d["CompanyName"]))


async def crawl_history_trades(
    stock_id: str, date: str, session: aiohttp.ClientSession
) -> None:
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_id}"
        async with session.get(url, headers={"User-Agent": ua.random}) as resp:
            data: Dict[str, Any] = await resp.json()
            if data["total"] == 0:
                return
            history_trades = [HistoryTrade.parse(d, stock_id) for d in data["data"]]
            for trade in history_trades:
                await ignore_conflict_create(trade)
    except Exception as e:
        logging.error(
            f"An error occurred while crawling history trades for {stock_id} on date {date}"
        )
        logging.exception(e)


async def main():
    await Tortoise.init(
        db_url=os.getenv("DB_URL") or "sqlite://db.sqlite3",
        modules={"models": ["models"]},
    )
    await Tortoise.generate_schemas()

    today = get_today()
    date = today.strftime("%Y%m%d")

    async with aiohttp.ClientSession() as session:
        if args.id:
            stock_ids = [args.id]
        else:
            await crawl_stocks(session)
            stock_ids = [stock.id for stock in await Stock.all()]

        for stock_id in stock_ids:
            if len(stock_id) != 4:
                continue

            if args.year == 0:
                logging.info(f"Start crawling {stock_id} on date {date}")
                await crawl_history_trades(stock_id, date, session)
            else:
                max_month = 13
                if args.year == today.year:
                    max_month = today.month + 1
                for month in range(1, max_month):
                    logging.info(f"Start crawling {stock_id} on {args.year}-{month}")
                    month_str = str(month).zfill(2)
                    await crawl_history_trades(
                        stock_id, f"{args.year}{month_str}01", session
                    )

            await asyncio.sleep(0.5)


run_async(main())
