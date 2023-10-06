from fastapi import FastAPI
from tortoise import Tortoise

from models import HistoryTrade

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
    return {"message": "Stock API v1.0"}


@app.get("/history_trades")
async def history_trades(stock_id: str, num: int):
    return (
        await HistoryTrade.filter(stock_id=stock_id)
        .order_by("-date")
        .limit(num)
        .all()
        .values()
    )
