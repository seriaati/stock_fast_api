import datetime

from tortoise.exceptions import IntegrityError
from tortoise.models import Model as TortoiseModel


def roc_to_western_date(roc_date_str: str) -> datetime.date:
    """將民國年轉換成西元年"""
    # format: 1100101
    year = roc_date_str[:3]
    month = roc_date_str[3:5]
    day = roc_date_str[5:]
    return datetime.date(int(year) + 1911, int(month), int(day))


def get_now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=8)))


def get_today() -> datetime.date:
    return get_now().date()


async def ignore_conflict_create(obj: TortoiseModel) -> None:
    try:
        await obj.save()
    except IntegrityError:
        pass


def float_string_to_int(s: str) -> int:
    if s == "--":
        return 0
    return int(float(remove_comma(s)))


def remove_comma(s: str) -> str:
    return s.replace(",", "")
