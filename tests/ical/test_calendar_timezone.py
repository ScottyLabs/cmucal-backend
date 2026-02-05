from icalendar import Calendar
from zoneinfo import ZoneInfo
from datetime import datetime

from app.services.ical import (
    get_calendar_timezone,
    normalize_ics_datetime,
    import_ical_feed_using_helpers,
)
from app.models.models import Event, EventOccurrence


def test_get_calendar_timezone_from_x_wr():
    ics = """BEGIN:VCALENDAR
VERSION:2.0
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:test-1
DTSTART:20260914T180000
END:VEVENT
END:VCALENDAR
"""
    cal = Calendar.from_ical(ics)
    tz = get_calendar_timezone(cal)

    assert tz == ZoneInfo("America/New_York")


def test_normalize_floating_dtstart():
    raw = datetime(2026, 9, 14, 18, 0)  # floating
    tz = ZoneInfo("America/New_York")

    dt = normalize_ics_datetime(raw, tz)

    assert dt.tzinfo == tz
    assert dt.hour == 18


def test_gcal_import_event_time(
    db, org_factory, category_factory, user_factory, calendar_source_factory
):
    org = org_factory()
    category = category_factory(org_id=org.id)
    user = user_factory()

    source = calendar_source_factory(org=org, category=category)

    ics = """BEGIN:VCALENDAR
VERSION:2.0
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:test-gcal-1
SUMMARY:Test Event
DTSTART:20260914T180000
DTEND:20260914T220000
END:VEVENT
END:VCALENDAR
"""

    result = import_ical_feed_using_helpers(
        db_session=db,
        ical_text_or_url=ics,
        org_id=org.id,
        category_id=category.id,
        calendar_source_id=source.id,  # ✅ REQUIRED
        default_event_type="CLUB",
        user_id=user.id,
    )

    assert result["success"] is True

    event = db.query(Event).filter_by(ical_uid="test-gcal-1").one()

    assert event.event_timezone is not None
    assert event.start_datetime.hour == 22  # 18 EDT → 22 UTC

    local = event.start_datetime.astimezone(ZoneInfo("America/New_York"))
    assert local.hour == 18


def test_gcal_weekly_recurrence_hour_stable(
    db, org_factory, category_factory, user_factory, calendar_source_factory
):
    org = org_factory()
    category = category_factory(org_id=org.id)
    user = user_factory()
    source = calendar_source_factory(org=org, category=category)

    ics = """BEGIN:VCALENDAR
VERSION:2.0
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:test-gcal-rrule
SUMMARY:Weekly Rehearsal
DTSTART:20260914T180000
DTEND:20260914T200000
RRULE:FREQ=WEEKLY;COUNT=3
END:VEVENT
END:VCALENDAR
"""

    import_ical_feed_using_helpers(
        db_session=db,
        ical_text_or_url=ics,
        org_id=org.id,
        category_id=category.id,
        calendar_source_id=source.id,
        default_event_type="CLUB",
        user_id=user.id,
    )

    occs = db.query(EventOccurrence).order_by(EventOccurrence.start_datetime).all()
    assert len(occs) == 3

    for occ in occs:
        local = occ.start_datetime.astimezone(ZoneInfo("America/New_York"))
        assert local.hour == 18


def test_recurrence_override_matches_exact_hour(
    db, org_factory, category_factory, user_factory, calendar_source_factory
):
    org = org_factory()
    category = category_factory(org_id=org.id)
    user = user_factory()
    source = calendar_source_factory(org=org, category=category)

    ics = """BEGIN:VCALENDAR
VERSION:2.0
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:test-override
DTSTART:20260914T180000
RRULE:FREQ=WEEKLY;COUNT=2
END:VEVENT
BEGIN:VEVENT
UID:test-override
RECURRENCE-ID:20260921T180000
DTSTART:20260921T190000
END:VEVENT
END:VCALENDAR
"""

    import_ical_feed_using_helpers(
        db_session=db,
        ical_text_or_url=ics,
        org_id=org.id,
        category_id=category.id,
        calendar_source_id=source.id,
        default_event_type="CLUB",
        user_id=user.id,
    )

    occs = db.query(EventOccurrence).order_by(EventOccurrence.start_datetime).all()

    hours = [
        occ.start_datetime.astimezone(ZoneInfo("America/New_York")).hour
        for occ in occs
    ]

    assert hours == [18, 19]
