from app.models.models import Event

# from icalendar import Calendar, Event as IcalEvent
# from recurring_ical_events import recurring_ical_events
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import requests
from app.utils.date import ensure_aware_datetime, infer_semester_from_datetime
from app.models.models import (
    Event,
    RecurrenceRule,
    EventOccurrence,
    RecurrenceRdate,
    RecurrenceExdate,
    EventOverride,
)


### need to check type of start_datetime, end_datetime before using them
def save_event(
    db,
    org_id: int,
    category_id: int,
    title: str,
    start_datetime: str,
    end_datetime: str,
    is_all_day: bool,
    event_timezone: str,
    user_edited: List[int],
    semester: str = "unknown",
    description: str = None,
    location: str = None,
    source_url: str = None,
    event_type: str = None,
    calendar_source_id=None,
    ical_uid=None,
    ical_sequence=None,
    ical_last_modified=None,
):
    """
    Create a new event in the database.
    Args:
        db: Database session.
        org_id: ID of the organization.
        category_id: ID of the category.
        title: Title of the event.
        description: Description of the event (optional).
        start_datetime: Start datetime of the event.
        end_datetime: End datetime of the event.
        is_all_day: Whether the event is an all-day event.
        location: Location of the event (optional).
        source_url: Source URL for the event (optional).
        user_edited: A list of user IDs who has edited/uploaded the event.
    Returns:
        The created Event object.
    """
    start_dt = ensure_aware_datetime(start_datetime)
    end_dt = ensure_aware_datetime(end_datetime)

    if semester is None:
        semester = infer_semester_from_datetime(start_datetime)

    event = Event(
        org_id=org_id,
        category_id=category_id,
        title=title,
        description=description,
        start_datetime=start_dt,
        end_datetime=end_dt,
        is_all_day=is_all_day,
        location=location,
        semester=semester,
        source_url=source_url,
        event_type=event_type,
        user_edited=user_edited,
        event_timezone=event_timezone,
        calendar_source_id=calendar_source_id,
        ical_uid=ical_uid,
        ical_sequence=ical_sequence,
        ical_last_modified=ical_last_modified,
    )
    db.add(event)
    db.flush()  # Allocate event.id without committing
    return event


def get_event_by_id(db, event_id: int):
    """
    Retrieve an event by its ID.
    Args:
        db: Database session.
        event_id: ID of the event.
    Returns:
        The Event object if found, otherwise None.
    """
    return db.query(Event).filter(Event.id == event_id).first()


def delete_event(db, event_id: int):
    """
    Delete an event by its ID.
    Args:
        db: Database session.
        event_id: ID of the event to delete.
    Returns:
        True if the event was deleted, False if it was not found.
    """
    event = db.query(Event).filter(Event.id == event_id).first()
    if event:
        db.delete(event)
        return True
    return False


def get_events_by_org(db, org_id: int):
    """
    Retrieve all events for a given organization.
    Args:
        db: Database session.
        org_id: ID of the organization.
    Returns:
        A list of Event objects.
    """
    return db.query(Event).filter(Event.org_id == org_id).all()
