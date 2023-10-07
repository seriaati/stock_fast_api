from fastapi import FastAPI
from tortoise import Tortoise

from models import HistoryTrade, Stock

app = FastAPI()


@app.on_event("startup")
async def startup():
    await Tortoise.init(db_url="sqlite://db.sqlite3", modules={"models": ["models"]})
    await Tortoise.generate_schemas()


@app.on_event("shutdown")
async def shutdown():
    await Tortoise.close_connections()


@app.get("/")
async def root():
    return {"message": "Stock Fast API v1.1.0"}


@app.get("/history_trades/{stock_id}")
async def stock_history_trades(stock_id: str, limit: int = 1):
    return (
        await HistoryTrade.filter(stock_id=stock_id)
        .order_by("-date")
        .limit(limit)
        .all()
        .values()
    )


@app.get("/stocks")
async def stocks():
    return await Stock.all().values()


@app.get("/stocks/{stock_id}")
async def stock_detail(stock_id: str):
    return await Stock.get(id=stock_id).values()
