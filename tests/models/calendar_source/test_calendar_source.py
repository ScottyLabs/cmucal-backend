from app.models import Event, CalendarSource
from app.models.calendar_source import deactivate_calendar_source
from app.services.ical import delete_events_for_calendar_source


def test_delete_events_for_calendar_source(
    db, calendar_source_factory, event_factory
):
    source = calendar_source_factory(active=True)

    events = event_factory.create_batch(
        3,
        calendar_source_id=source.id,
        source_url=source.url,
    )

    deleted_ids = delete_events_for_calendar_source(db, source.id)
    db.commit()

    assert len(deleted_ids) == 3
    assert db.query(Event).count() == 0

    refreshed = db.get(CalendarSource, source.id)
    assert refreshed.active is False


def test_deactivate_calendar_source(db, calendar_source_factory):
    source = calendar_source_factory(active=True)

    deactivate_calendar_source(db, source.id)
    db.commit()

    refreshed = db.get(CalendarSource, source.id)
    assert refreshed.active is False
