import datetime
from typing import List, Optional, Tuple


SEMESTER_CONFIG = {
    "Spring": {
        "layout": "sched_layout_spring",
        "start": (1, 12),
        "end": (5, 5),
    },
    "Summer1": {
        "layout": "sched_layout_summer_1",
        "start": (5, 11),
        "end": (6, 18),
    },
    "Summer2": {
        "layout": "sched_layout_summer_2",
        "start": (6, 22),
        "end": (7, 31),
    },
    "Fall": {
        "layout": "sched_layout_fall",
        "start": (8, 25),
        "end": (12, 15),
    },
}


def get_current_semester(
    semester_label: str,
) -> Tuple[str, str, datetime.datetime, datetime.datetime]:
    """
    Resolve a semester label like 'Spring_26' into:
        (soc_layout, semester_label, semester_start, semester_end)
    """

    try:
        name, year_suffix = semester_label.split("_")
        year = 2000 + int(year_suffix)
    except Exception:
        raise ValueError(
            f"Invalid semester label '{semester_label}'. Expected format like 'Spring_26'"
        )

    if name not in SEMESTER_CONFIG:
        raise ValueError(f"Unknown semester name '{name}'. Should be in {list(SEMESTER_CONFIG.keys())}")

    config = SEMESTER_CONFIG[name]

    start_month, start_day = config["start"]
    end_month, end_day = config["end"]

    start = datetime.datetime(year, start_month, start_day)
    end = datetime.datetime(year, end_month, end_day)

    return (
        config["layout"],
        semester_label,
        start,
        end,
    )


def infer_soc_semester_labels(
    today: Optional[datetime.date] = None,
) -> List[str]:
    """
    SOC publishes separate Spring and Fall layouts. Return labels for the
    current calendar window and the next major term so cron runs stay fresh
    without manual edits.

    - Jan–May: Spring (this year) then Fall (this year).
    - Jun–Jul: Spring (this year, just ended / winding down) then Fall (this year).
    - Aug–Dec: Fall (this year) then Spring (next year), e.g. December → Fall + Spring.

    Labels match get_current_semester format, e.g. Spring_26, Fall_25.
    """
    d = today or datetime.date.today()
    y = d.year
    yy = f"{y % 100:02d}"

    if d.month >= 8:
        fall = f"Fall_{yy}"
        spring_next = f"Spring_{(y + 1) % 100:02d}"
        return [fall, spring_next]
    if 1 <= d.month <= 5:
        spring = f"Spring_{yy}"
        fall = f"Fall_{yy}"
        return [spring, fall]
    # June–July: upcoming Fall and the Spring that just ended (same calendar year).
    spring = f"Spring_{yy}"
    fall = f"Fall_{yy}"
    return [spring, fall]
