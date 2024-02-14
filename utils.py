import datetime


def roc_to_western_date(roc_date_str: str) -> datetime.date:
    """將民國年轉換成西元年"""
    # format: 1100101
    roc_date_str = roc_date_str.replace("/", "").replace("*", "")
    year = roc_date_str[:3]
    month = roc_date_str[3:5]
    day = roc_date_str[5:]
    return datetime.date(int(year) + 1911, int(month), int(day))


def get_now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=8)))


def get_today() -> datetime.date:
    return get_now().date()


def string_to_float(s: str) -> float:
    if s == "--":
        return 0
    return float(remove_comma(s))


def remove_comma(s: str) -> str:
    return s.replace(",", "")
