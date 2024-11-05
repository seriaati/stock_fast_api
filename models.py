import datetime
from typing import List, Self

from seria.tortoise.model import Model
from tortoise import fields
from tortoise.exceptions import IntegrityError

from utils import remove_comma, roc_to_western_date, string_to_float


class BaseModel(Model):
    def __str__(self) -> str:
        return f"{self.__class__.__name__}({', '.join(f'{field}={getattr(self, field)!r}' for field in self._meta.db_fields if hasattr(self, field))})"

    def __repr__(self) -> str:
        return str(self)

    class Meta:
        abstract = True


class HistoryTrade(BaseModel):
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
    def parse_tpex(cls, data: List[str], date: datetime.date) -> Self:
        return cls(
            date=date,
            stock_id=data[0],
            close_price=string_to_float(data[2]),
            open_price=string_to_float(data[4]),
            high_price=string_to_float(data[5]),
            low_price=string_to_float(data[6]),
            total_volume=int(remove_comma(data[7])),
            total_value=int(remove_comma(data[8])),
        )

    async def create_or_update(self) -> None:
        try:
            await self.save()
        except IntegrityError:
            await HistoryTrade.filter(date=self.date, stock_id=self.stock_id).update(
                close_price=self.close_price,
                open_price=self.open_price,
                high_price=self.high_price,
                low_price=self.low_price,
                total_volume=self.total_volume,
                total_value=self.total_value,
            )


class Stock(BaseModel):
    id = fields.CharField(max_length=10, pk=True)
    name = fields.CharField(max_length=10)
