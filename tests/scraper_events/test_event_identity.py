import pytest
from datetime import datetime
from dateutil.parser import isoparse

from scraper.helpers.event import event_identity, normalize_dt


def test_event_identity_datetime_roundtrip():
    """
    Event identity must survive:
    - naive datetime
    - ISO string
    - timezone-aware datetime
    """

    org_id = 42
    title = "48105 A"
    semester = "Spring_26"
    location = "Pittsburgh, Pennsylvania"

    naive_start = datetime(2026, 1, 12, 14, 0)
    naive_end = datetime(2026, 1, 12, 16, 50)

    iso_start = naive_start.isoformat() + "+00:00"
    iso_end = naive_end.isoformat() + "+00:00"

    identity_naive = event_identity(
        org_id,
        title,
        semester,
        naive_start,
        naive_end,
        location,
    )

    identity_iso = event_identity(
        org_id,
        title,
        semester,
        iso_start,
        iso_end,
        location,
    )

    assert identity_naive == identity_iso

def test_event_identity_location_is_distinct():
    org_id = 42
    title = "48105 A"
    semester = "Spring_26"
    start = datetime(2026, 1, 12, 14, 0)
    end = datetime(2026, 1, 12, 16, 50)

    loc1 = "Pittsburgh, Pennsylvania"
    loc2 = "Tepper Quad"

    id1 = event_identity(org_id, title, semester, start, end, loc1)
    id2 = event_identity(org_id, title, semester, start, end, loc2)

    assert id1 != id2

def test_event_identity_location_whitespace():
    org_id = 42
    title = "48105 A"
    semester = "Spring_26"
    start = datetime(2026, 1, 12, 14, 0)
    end = datetime(2026, 1, 12, 16, 50)

    id1 = event_identity(
        org_id, title, semester, start, end, "Pittsburgh, Pennsylvania"
    )
    id2 = event_identity(
        org_id, title, semester, start, end, "Pittsburgh, Pennsylvania "
    )

    assert id1 == id2

def test_rrule_identity_matches_event_identity():
    event = {
        "org_id": 99,
        "title": "15112 B",
        "semester": "Spring_26",
        "start_datetime": datetime(2026, 1, 13, 9, 0),
        "end_datetime": datetime(2026, 1, 13, 9, 50),
        "location": "DH 2210",
    }

    rrule = {
        "org_id": 99,
        "title": "15112 B",
        "semester": "Spring_26",
        "start_datetime": event["start_datetime"].isoformat(),
        "end_datetime": event["end_datetime"].isoformat(),
        "location": "DH 2210",
    }

    event_id = event_identity(**event)
    rrule_id = event_identity(**rrule)

    assert event_id == rrule_id