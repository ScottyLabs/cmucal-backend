import pytest
from zoneinfo import ZoneInfo
from datetime import datetime

from scraper.helpers.timezone import timezone_from_location, DEFAULT_TZ
from scraper.transforms.soc_events import build_events_and_rrules
from scraper.models import ScheduleOfClasses


@pytest.mark.parametrize(
    "location,expected",
    [
        ("Pittsburgh, Pennsylvania", "America/New_York"),
        ("New York, New York", "America/New_York"),
        ("Washington, District of Columbia", "America/New_York"),
        ("Washington, District of Columbia:Dulles", "America/New_York"),
        ("Los Angeles, California", "America/Los_Angeles"),
        ("San Jose, California", "America/Los_Angeles"),
        ("Doha, Qatar", "Asia/Qatar"),
        ("Kigali, Rwanda", "Africa/Kigali"),
        ("Lisbon, Portugal", "Europe/Lisbon"),
    ],
)
def test_timezone_from_location_known(location, expected):
    tz = timezone_from_location(location)
    assert isinstance(tz, ZoneInfo)
    assert tz.key == expected


def test_timezone_from_location_unknown_fallback():
    tz = timezone_from_location("Some Random Place")
    assert tz == DEFAULT_TZ


def test_timezone_from_location_none_fallback():
    tz = timezone_from_location(None)
    assert tz == DEFAULT_TZ

# Integration test to ensure build_events_and_rrules creates timezone-aware datetimes
SEM_START = datetime(2026, 1, 12)
SEM_END = datetime(2026, 5, 5)
def make_soc(location="Pittsburgh, Pennsylvania"):
    return ScheduleOfClasses(
        id=1,
        course_num="15112",
        course_name="Foundations of Programming",
        lecture_section="A",
        lecture_days="MWF",
        lecture_time_start="09:00AM",
        lecture_time_end="09:50AM",
        location=location,
        semester="Spring_26",
        sem_start=SEM_START,
        sem_end=SEM_END,
    )


def test_build_events_timezone_aware():
    soc = make_soc()
    org_id_by_key = {("15112", "Spring_26"): 1}
    category_id_by_org = {1: 10}

    events, rrules = build_events_and_rrules(
        [soc],
        org_id_by_key,
        category_id_by_org,
    )

    event = events[0]

    assert event["start_datetime"].tzinfo is not None
    assert event["end_datetime"].tzinfo is not None
    assert event["start_datetime"].tzinfo.key == "America/New_York"

def test_dst_transition_new_york():
    soc = make_soc()
    org_id_by_key = {("15112", "Spring_26"): 1}
    category_id_by_org = {1: 10}

    events, _ = build_events_and_rrules(
        [soc],
        org_id_by_key,
        category_id_by_org,
    )

    start = events[0]["start_datetime"]

    # January 12, 2026 should be EST (UTC-5)
    assert start.utcoffset().total_seconds() == -5 * 3600
    assert start.isoformat().endswith("-05:00")
    expected = datetime(
        2026, 1, 12, 9, 0,
        tzinfo=ZoneInfo("America/New_York")
    )
    assert start == expected


    # Now test a date in daylight saving time
    soc.sem_start = datetime(2026, 3, 30)  # After
    events, _ = build_events_and_rrules(
        [soc],
        org_id_by_key,
        category_id_by_org,
    )
    start = events[0]["start_datetime"]
    # March 30, 2026 should be EDT (UTC-4)
    assert start.utcoffset().total_seconds() == -4 * 3600
    assert start.isoformat().endswith("-04:00")
    expected = datetime(
        2026, 3, 30, 9, 0,
        tzinfo=ZoneInfo("America/New_York")
    )
    assert start == expected

def test_event_and_rrule_timezone_consistency():
    soc = make_soc("Pittsburgh, Pennsylvania")

    org_id_by_key = {("15112", "Spring_26"): 1}
    category_id_by_org = {1: 10}

    events, rrules = build_events_and_rrules(
        [soc],
        org_id_by_key,
        category_id_by_org,
    )

    event = events[0]
    rrule = rrules[0]

    # --- Event checks ---
    assert event["start_datetime"].tzinfo is not None
    assert event["end_datetime"].tzinfo is not None
    assert event["start_datetime"].tzinfo.key == "America/New_York"

    # --- RRULE checks ---
    assert rrule["start_datetime"].tzinfo is not None
    assert rrule["start_datetime"].tzinfo.key == "America/New_York"

    # RRULE should match event local wall-clock time
    assert rrule["start_datetime"].hour == event["start_datetime"].hour
    assert rrule["start_datetime"].minute == event["start_datetime"].minute

def test_rrule_dst_transition_new_york():
    soc = make_soc("Pittsburgh, Pennsylvania")

    org_id_by_key = {("15112", "Spring_26"): 1}
    category_id_by_org = {1: 10}

    # ---- Pre-DST (EST) ----
    soc.sem_start = datetime(2026, 1, 12)  # EST
    events, rrules = build_events_and_rrules(
        [soc],
        org_id_by_key,
        category_id_by_org,
    )

    rrule_start = rrules[0]["start_datetime"]

    assert rrule_start.utcoffset().total_seconds() == -5 * 3600
    assert rrule_start.isoformat().endswith("-05:00")
    assert rrule_start.hour == 9

    # ---- Post-DST (EDT) ----
    soc.sem_start = datetime(2026, 3, 30)  # EDT
    events, rrules = build_events_and_rrules(
        [soc],
        org_id_by_key,
        category_id_by_org,
    )

    rrule_start = rrules[0]["start_datetime"]

    assert rrule_start.utcoffset().total_seconds() == -4 * 3600
    assert rrule_start.isoformat().endswith("-04:00")
    assert rrule_start.hour == 9
