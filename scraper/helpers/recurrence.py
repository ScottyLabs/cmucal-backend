from datetime import datetime, time
from typing import Optional
from zoneinfo import ZoneInfo
from app.models import FrequencyType
from scraper.models import ScheduleOfClasses

from datetime import datetime, time
from typing import Optional
from app.models import FrequencyType

SOC_DAY_MAP = {
    "M": "MO",
    "T": "TU",
    "W": "WE",
    "R": "TH",
    "F": "FR",
    "S": "SA",
    "U": "SU",
}


def parse_soc_time(t: str) -> time:
    return datetime.strptime(t, "%I:%M%p").time()


def build_rrule_from_parts(
    *,
    lecture_days: str,
    sem_start,
    sem_end,
    start_time: time,
    tz: ZoneInfo,
) -> Optional[dict]:

    if not lecture_days:
        return None

    by_day = [SOC_DAY_MAP[d] for d in lecture_days]

    dtstart = datetime.combine(
        sem_start,
        start_time,
        tzinfo=tz,
    )

    until_dt = datetime.combine(
        sem_end,
        time(23, 59, 59),
        tzinfo=tz,
    )

    return {
        "frequency": FrequencyType.WEEKLY,
        "interval": 1,
        "by_day": by_day,
        "until": until_dt,
        "start_datetime": dtstart,
        "count": None,
        "by_month": None,
        "by_month_day": None,
        "orig_until": until_dt,
    }