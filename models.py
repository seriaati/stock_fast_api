from typing import List, Self

from tortoise import fields
from tortoise.models import Model

from utils import float_string_to_int, remove_comma, roc_to_western_date


class HistoryTrade(Model):
    date = fields.DateField()
    stock_id = fields.CharField(max_length=10)

    total_volume = fields.BigIntField()
    total_value = fields.BigIntField()

    open_price = fields.IntField()
    high_price = fields.IntField()
    low_price = fields.IntField()
    close_price = fields.IntField()

    class Meta:
        unique_together = ("date", "stock_id")

    @classmethod
    def parse(cls, data: List[str], stock_id: str) -> Self:
        return cls(
            date=roc_to_western_date(data[0].replace("/", "")),
            stock_id=stock_id,
            total_volume=int(remove_comma(data[1])),
            total_value=int(remove_comma(data[2])),
            open_price=float_string_to_int(data[3]),
            high_price=float_string_to_int(data[4]),
            low_price=float_string_to_int(data[5]),
            close_price=float_string_to_int(data[6]),
        )


class Stock(Model):
    id = fields.CharField(max_length=10, pk=True)
    name = fields.CharField(max_length=10)
