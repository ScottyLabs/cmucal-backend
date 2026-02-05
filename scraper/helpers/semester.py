import datetime
from typing import Optional, Tuple


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
