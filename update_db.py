import argparse
from typing import Any, Dict, List

import aiohttp
from tortoise import Tortoise, run_async

from models import HistoryTrade
from utils import bulk_update_or_create, get_today

parser = argparse.ArgumentParser()
parser.add_argument("--init", action="store_true")
args = parser.parse_args()


async def fetch_stock_ids(session: aiohttp.ClientSession) -> List[str]:
    """
    取得上市公司代號與上櫃公司代號
    """
    stock_ids: List[str] = []
    async with session.get(
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    ) as resp:  # 取得所有上市公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["公司代號"].isdigit():
                stock_id = d["公司代號"]
                stock_ids.append(stock_id)

    async with session.get(
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
    ) as resp:  # 取得所有上櫃公司代號
        data: List[Dict[str, str]] = await resp.json()
        for d in data:
            if d["SecuritiesCompanyCode"].isdigit():
                stock_id = d["SecuritiesCompanyCode"]
                stock_ids.append(stock_id)

    return stock_ids


async def crawl_and_save_history_trades(
    stock_id: str, date: str, session: aiohttp.ClientSession
) -> None:
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={date}&stockNo={stock_id}"
    async with session.get(url) as resp:
        data: Dict[str, Any] = await resp.json()
        if data["total"] == 0:
            return
        history_trades = [HistoryTrade.parse(d, stock_id) for d in data["data"]]
        await bulk_update_or_create(history_trades)


async def main():
    await Tortoise.init(db_url="sqlite://db.sqlite3", modules={"models": ["models"]})
    await Tortoise.generate_schemas()

    today = get_today()
    date = today.strftime("%Y%m%d")

    async with aiohttp.ClientSession() as session:
        stock_ids = await fetch_stock_ids(session)
        for stock_id in stock_ids:
            if len(stock_id) != 4:
                continue
            print("stock_id:", stock_id)

            if args.init:
                for month in range(1, 13):
                    month_str = str(month).zfill(2)
                    await crawl_and_save_history_trades(
                        stock_id, f"{today.year}{month_str}01", session
                    )
            else:
                await crawl_and_save_history_trades(stock_id, date, session)


run_async(main())
