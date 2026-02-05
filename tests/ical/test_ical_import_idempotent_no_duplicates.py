import pytest
from app.models.models import Event, CalendarSource
from app.services.ical import import_ical_feed_using_helpers
from app.models.calendar_source import create_calendar_source
from datetime import datetime, timezone, timedelta

dtstamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
dtstart = (
    datetime.now(timezone.utc) + timedelta(days=1)
).strftime("%Y%m%dT%H%M%SZ")

SIMPLE_ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Test//EN
BEGIN:VEVENT
UID:test-uid-123
DTSTAMP:{dtstamp}
DTSTART:{dtstart}
DTEND:{dtstart}
SUMMARY:Test Event
DESCRIPTION:Hello world
LOCATION:Test Room
END:VEVENT
END:VCALENDAR
"""


def test_ical_import_idempotent_no_duplicates(db, org_factory, category_factory, user_factory):
    """
    Importing the same ICS twice for the same CalendarSource
    must NOT create duplicate Events.
    """

    org = org_factory()
    category = category_factory(org_id=org.id)
    user = user_factory()

    # Create CalendarSource
    calendar_source = create_calendar_source(
        db_session=db,
        url="https://example.com/test.ics",
        org_id=org.id,
        category_id=category.id,
        active=True,
        created_by_user_id=user.id,
    )
    db.flush()

    # First import
    result1 = import_ical_feed_using_helpers(
        db_session=db,
        ical_text_or_url=SIMPLE_ICS,
        calendar_source_id=calendar_source.id,
        org_id=org.id,
        category_id=category.id,
        user_id=user.id,
        source_url=calendar_source.url,
    )
    assert result1["success"] is True

    events_after_first = (
        db.query(Event)
        .filter(Event.calendar_source_id == calendar_source.id)
        .all()
    )
    assert len(events_after_first) == 1
    first_event_id = events_after_first[0].id

    # Second import (same ICS)
    result2 = import_ical_feed_using_helpers(
        db_session=db,
        ical_text_or_url=SIMPLE_ICS,
        calendar_source_id=calendar_source.id,
        org_id=org.id,
        category_id=category.id,
        user_id=user.id,
        source_url=calendar_source.url,
    )
    assert result2["success"] is True

    events_after_second = (
        db.query(Event)
        .filter(Event.calendar_source_id == calendar_source.id)
        .all()
    )

    # THE CRITICAL ASSERTION
    assert len(events_after_second) == 1
    assert events_after_second[0].id == first_event_id
