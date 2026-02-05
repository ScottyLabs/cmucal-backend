import pytest
from datetime import datetime, timezone
import uuid

from app.models.models import CalendarSource


@pytest.fixture
def calendar_source_factory(db, org_factory, category_factory):
    def create_calendar_source(**kwargs):
        org = kwargs.pop("org", None) or org_factory()
        category = kwargs.pop("category", None) or category_factory(org_id=org.id)

        source = CalendarSource(
            url=kwargs.pop(
                "url",
                f"https://calendar.google.com/calendar/ical/test/{uuid.uuid4()}.ics"
            ),
            org_id=kwargs.pop("org_id", org.id),
            category_id=kwargs.pop("category_id", category.id),
            active=kwargs.pop("active", True),
            sync_mode=kwargs.pop("sync_mode", "delta"),
            fetch_interval_seconds=kwargs.pop("fetch_interval_seconds", 3600),
            default_event_type=kwargs.pop("default_event_type", "CLUB"),
            notes=kwargs.pop("notes", None),
            created_by_user_id=kwargs.pop("created_by_user_id", None),
            last_sync_status=kwargs.pop("last_sync_status", None),
            content_hash=kwargs.pop("content_hash", None),
            etag=kwargs.pop("etag", None),
            last_modified_hdr=kwargs.pop("last_modified_hdr", None),
            locked_at=kwargs.pop("locked_at", None),
            lock_owner=kwargs.pop("lock_owner", None),
            created_at=kwargs.pop("created_at", datetime.now(timezone.utc)),
            updated_at=kwargs.pop("updated_at", datetime.now(timezone.utc)),
            **kwargs,
        )

        db.add(source)
        db.flush()
        return source

    return create_calendar_source
