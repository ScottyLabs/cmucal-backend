# scraper/helpers/timezone.py
from zoneinfo import ZoneInfo

DEFAULT_TZ = ZoneInfo("America/New_York")

LOCATION_TZ_MAP = {
    "doha, qatar": ZoneInfo("Asia/Qatar"),
    "kigali, rwanda": ZoneInfo("Africa/Kigali"),
    "lisbon, portugal": ZoneInfo("Europe/Lisbon"),

    "los angeles, california": ZoneInfo("America/Los_Angeles"),
    "san jose, california": ZoneInfo("America/Los_Angeles"),

    "new york, new york": ZoneInfo("America/New_York"),
    "pittsburgh, pennsylvania": ZoneInfo("America/New_York"),
    "washington, district of columbia": ZoneInfo("America/New_York"),
    "washington, district of columbia:dulles": ZoneInfo("America/New_York"),
}

def timezone_from_location(location: str | None):
    if not location:
        return DEFAULT_TZ

    key = location.strip().lower()

    return LOCATION_TZ_MAP.get(key, DEFAULT_TZ)
