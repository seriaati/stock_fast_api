from typing import List, Self

from tortoise import fields
from tortoise.models import Model

from utils import remove_comma, roc_to_western_date, string_to_float


class HistoryTrade(Model):
    date = fields.DateField()
    stock_id = fields.CharField(max_length=10)

    total_volume = fields.BigIntField()
    total_value = fields.BigIntField()

    open_price = fields.FloatField()
    high_price = fields.FloatField()
    low_price = fields.FloatField()
    close_price = fields.FloatField()

    class Meta:
        unique_together = ("date", "stock_id")

    @classmethod
    def parse_twse(cls, data: List[str], stock_id: str) -> Self:
        return cls(
            date=roc_to_western_date(data[0]),
            stock_id=stock_id,
            total_volume=int(remove_comma(data[1])),
            total_value=int(remove_comma(data[2])),
            open_price=string_to_float(data[3]),
            high_price=string_to_float(data[4]),
            low_price=string_to_float(data[5]),
            close_price=string_to_float(data[6]),
        )

    @classmethod
    def parse_tpex(cls, data: List[str], stock_id: str) -> Self:
        return cls(
            date=roc_to_western_date(data[0].replace("/", "")),
            stock_id=stock_id,
            total_volume=int(remove_comma(data[1])) * 1000,
            total_value=int(remove_comma(data[2])) * 1000,
            open_price=string_to_float(data[3]),
            high_price=string_to_float(data[4]),
            low_price=string_to_float(data[5]),
            close_price=string_to_float(data[6]),
        )


class Stock(Model):
    id = fields.CharField(max_length=10, pk=True)
    name = fields.CharField(max_length=10)
