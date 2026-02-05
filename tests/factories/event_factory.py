from zoneinfo import ZoneInfo
import pytest
from datetime import datetime, timezone, timedelta

from app.models.models import Event

import uuid

@pytest.fixture
def event_factory(db, org_factory, category_factory):
    def create_event(**kwargs):
        org = kwargs.pop("org", None) or org_factory()
        category = kwargs.pop("category", None) or category_factory(org_id=org.id)
        calendar_source_id = kwargs.pop("calendar_source_id", None)
        event_timezone = kwargs.pop("event_timezone", "America/New_York")
        event_tz = ZoneInfo(event_timezone)

        start = kwargs.pop(
            "start_datetime",
            datetime(2026, 1, 15, 18, 0, tzinfo=event_tz).astimezone(timezone.utc),
        )

        event = Event(
            org_id=org.id,
            category_id=category.id,
            calendar_source_id=calendar_source_id,
            title=kwargs.pop("title", "Test Event"),
            description=kwargs.pop("description", None),
            location=kwargs.pop("location", "Test Location"),

            start_datetime=start,
            end_datetime=kwargs.pop("end_datetime", start + timedelta(hours=2)),
            is_all_day=kwargs.pop("is_all_day", False),
            event_timezone=event_timezone,
            semester=kwargs.pop("semester", "Spring_26"),

            user_edited=kwargs.pop("user_edited", []),
            source_url=kwargs.pop("source_url", None),
            event_type=kwargs.pop("event_type", "CLUB"),
            ical_uid=kwargs.pop(
                "ical_uid",
                f"test-{uuid.uuid4()}"
            ),
            ical_sequence=kwargs.pop("ical_sequence", 0),
            ical_last_modified=kwargs.pop("ical_last_modified", None),

            last_updated_at=kwargs.pop("last_updated_at", datetime.now(timezone.utc)),
        )

        db.add(event)
        db.flush()
        return event

    def create_batch(n, **kwargs):
        return [create_event(**kwargs) for _ in range(n)]

    create_event.create_batch = create_batch
    return create_event