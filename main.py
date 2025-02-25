import datetime
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from tortoise import Tortoise
from tortoise.exceptions import DoesNotExist

from models import HistoryTrade, Stock

load_dotenv()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[Any, Any]:
    await Tortoise.init(
        db_url=os.getenv("DB_URL") or "sqlite://db.sqlite3",
        modules={"models": ["models"]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "Stock Fast API v1.3.0"}


@app.get("/history_trades/{stock_id}")
async def stock_history_trades(
    stock_id: str,
    limit: Optional[int] = None,
    after_date: str | None = None,
    before_date: str | None = None,
):
    query = HistoryTrade.filter(stock_id=stock_id).order_by("-date")
    if limit is not None:
        query = query.limit(limit)
    if after_date is not None:
        query = query.filter(date__gte=after_date)
    if before_date is not None:
        query = query.filter(date__lte=before_date)

    return await query.all().values()


@app.get("/stocks")
async def stocks(name: Optional[str] = None):
    if name:
        try:
            return await Stock.get(name=name).values()
        except DoesNotExist:
            raise HTTPException(status_code=404, detail="Stock not found")
    return await Stock.all().values()


@app.get("/stocks/{stock_id}")
async def stock_detail(stock_id: str):
    try:
        return await Stock.get(id=stock_id).values()
    except DoesNotExist:
        raise HTTPException(status_code=404, detail="Stock not found")


if __name__ == "__main__":
    uvicorn.run("main:app", port=7080, log_level="info")
