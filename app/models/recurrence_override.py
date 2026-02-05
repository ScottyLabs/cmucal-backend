from app.models.enums import FrequencyType
from datetime import datetime, timedelta, timezone
from dateutil.rrule import (
    rrule,
    DAILY, WEEKLY, MONTHLY, YEARLY,
    MO, TU, WE, TH, FR, SA, SU,
    weekday,
)
from typing import List, Optional, Union
from dateutil.parser import parse as parse_datetime
from app.models.recurrence_rule import parse_by_day_array


def rrule_from_db_recurrence_override(override) -> rrule:
    """
    Constructs a dateutil.rrule object from a database RecurrenceOverride.
    
    RecurrenceOverride defines a pattern for matching occurrences (e.g., "all Tuesdays").
    The temporal bounds (start_datetime, count, until) come from the parent RecurrenceRule
    accessed via override.rrule relationship.
    
    Assumes `override` has attributes: frequency, interval,
    by_day (List[str]), by_month (int or List[int]), by_month_day (int or List[int]),
    and a relationship `rrule` to the parent RecurrenceRule.
    """
    freq_map = {
        'DAILY': DAILY,
        'WEEKLY': WEEKLY,
        'MONTHLY': MONTHLY,
        'YEARLY': YEARLY
    }

    # Fix: allow either Enum or string
    raw_freq = override.frequency.value if hasattr(override.frequency, "value") else override.frequency
    freq = freq_map.get(raw_freq)
    if freq is None:
        raise ValueError(f"Unsupported frequency: {override.frequency}")

    interval = override.interval or 1
    
    # Get temporal bounds from parent RecurrenceRule
    parent_rule = override.rrule
    start_datetime = parent_rule.start_datetime
    count = parent_rule.count
    until = parent_rule.until

    by_day_array = parse_by_day_array(override.by_day or [])
    by_month = override.by_month
    by_month_day = override.by_month_day

    kwargs = {
        "freq": freq,
        "dtstart": start_datetime,
        "interval": interval,
    }
    if count:
        kwargs["count"] = count
    if until:
        kwargs["until"] = until
    if by_day_array:
        kwargs["byweekday"] = by_day_array
    if by_month:
        kwargs["bymonth"] = [by_month] if isinstance(by_month, int) else by_month
    if by_month_day:
        kwargs["bymonthday"] = [by_month_day] if isinstance(by_month_day, int) else by_month_day

    return rrule(**kwargs)