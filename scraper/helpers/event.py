import hashlib
from datetime import datetime, date
from enum import Enum
from collections import defaultdict
from dateutil.parser import isoparse

def normalize_dt(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    return isoparse(value).replace(tzinfo=None)

def normalize_str(s: str) -> str:
    return s.strip() if s else ""

def group_soc_rows(soc_rows):
    grouped = defaultdict(list)

    for soc in soc_rows:
        key = (
            soc.course_num,
            soc.lecture_section,
            soc.semester,
            soc.lecture_time_start,
            soc.lecture_time_end,
            soc.location,
        )
        grouped[key].append(soc)
    print(f"Grouped {len(soc_rows)} SOC rows into {len(grouped)} event groups")

    return grouped

def event_identity(org_id, title, semester, start_datetime, end_datetime, location):
    return (
        org_id,
        normalize_str(title),
        normalize_str(semester),
        normalize_dt(start_datetime),
        normalize_dt(end_datetime),
        normalize_str(location),
    )

def json_safe(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def clean_row_for_insert(row: dict) -> dict:
    return {k: json_safe(v) for k, v in row.items()}


def format_time(value):
    """
    Accepts either:
    - datetime
    - ISO datetime string
    Returns: "HH:MMAM/PM"
    """
    if isinstance(value, (datetime, date)):
        dt = value
    else:
        dt = datetime.fromisoformat(value)

    return dt.strftime("%I:%M%p")
