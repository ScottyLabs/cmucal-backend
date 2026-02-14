from zoneinfo import ZoneInfo
from app.models.models import (
    Event, RecurrenceRule, EventOccurrence,
    RecurrenceExdate, RecurrenceRdate, EventOverride, RecurrenceOverride,
)
from app.models.enums import RecurrenceType
from app.models.recurrence_rule import get_rrule_from_db_rule
from app.models.recurrence_override import rrule_from_db_recurrence_override
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple, Optional
from copy import deepcopy
import enum
from app.utils.date import _ensure_aware, _parse_iso_aware, normalize_occurrence, normalize_set_to_tz


TRACE_EVENT_ID = None  # Set to an event ID to enable tracing
def trace(event, *msg):
    if event and event.id == TRACE_EVENT_ID:
        print("🧭 TRACE:", *msg)


def apply_overrides(
    occ_start: datetime,
    event: Event,
    duration: timedelta,
    overrides: Dict[datetime, EventOverride],
    recurrence_override_dates: Dict[datetime, RecurrenceOverride],
) -> Tuple[datetime, datetime, str, Optional[str], Optional[str]]:
    """
    Apply overrides to an occurrence datetime.
    
    Priority: EventOverride (date-specific) > RecurrenceOverride (pattern-based) > default event values
    
    Args:
        occ_start: The occurrence start datetime
        event: The parent Event object
        duration: The event duration
        overrides: Dict mapping dates to EventOverride objects
        recurrence_override_dates: Dict mapping dates to RecurrenceOverride objects
    
    Returns:
        Tuple of (start_dt, end_dt, title, description, location)
    """
    if occ_start in overrides:
        # Highest priority: date-specific EventOverride
        o = overrides[occ_start]
        start_dt = o.new_start or occ_start
        end_dt   = o.new_end or (start_dt + duration)
        title    = o.new_title or event.title
        desc     = o.new_description if o.new_description is not None else event.description
        loc      = o.new_location if o.new_location is not None else event.location

    elif occ_start in recurrence_override_dates:
        # Second priority: pattern-based RecurrenceOverride
        ro = recurrence_override_dates[occ_start]

        # For RecurrenceOverrides new_start/new_end are time-only adjustments
        # Apply the time portion to the occurrence date as stored new_start/new_end might 
        # only have a time portion and an arbitrary date portion
        if ro.new_start:
            start_dt = occ_start.replace(
                hour=ro.new_start.hour,
                minute=ro.new_start.minute,
                second=ro.new_start.second,
                microsecond=ro.new_start.microsecond
            )
        else:
            start_dt = occ_start

        if ro.new_end:
            end_dt = occ_start.replace(
                hour=ro.new_end.hour,
                minute=ro.new_end.minute,
                second=ro.new_end.second,
                microsecond=ro.new_end.microsecond
            )
        else:
            end_dt = start_dt + duration

        title = ro.new_title or event.title
        desc  = ro.new_description if ro.new_description is not None else event.description
        loc   = ro.new_location if ro.new_location is not None else event.location

    else:
        # Default: use event values
        start_dt = occ_start
        end_dt   = occ_start + duration
        title    = event.title
        desc     = event.description
        loc      = event.location
    
    start_dt = normalize_occurrence(start_dt, ZoneInfo(event.event_timezone))
    end_dt   = normalize_occurrence(end_dt, ZoneInfo(event.event_timezone))
    return start_dt, end_dt, title, desc, loc

def delete_event_occurrences_by_event_id(db, event_id: int):
    """
    Delete all event occurrences for a given event ID.

    Args:
        db: Database session.
        event_id: ID of the event whose occurrences are to be deleted.
    """
    db.query(EventOccurrence).filter_by(event_id=event_id).delete(synchronize_session=False)
    db.flush()

### need to check type of event_saved_at, start_datetime, end_datetime before using them
def save_event_occurrence(db, event_id: int, org_id: int, category_id: int, title: str, 
                          start_datetime, end_datetime, recurrence: RecurrenceType,
                          event_saved_at: str, event_timezone: str,
                          is_all_day: bool, user_edited: List[int], description: str = None, 
                          location: str = None, source_url: str = None):
    """
    Save an event occurrence in the database.

    Args:
        db: Database session.
        event_id: ID of the event.
        start_time: Start time of the occurrence in ISO format.
        end_time: End time of the occurrence in ISO format.

    Returns:
        The created EventOccurrence object.
    """
    event_tz = ZoneInfo(event_timezone)
    start_dt = _parse_iso_aware(start_datetime) if isinstance(start_datetime, str) else start_datetime.astimezone(event_tz)
    end_dt   = _parse_iso_aware(end_datetime)   if isinstance(end_datetime, str)   else end_datetime.astimezone(event_tz)
    saved_at = _parse_iso_aware(event_saved_at) if isinstance(event_saved_at, str) else event_saved_at

    start_dt = start_dt.astimezone(timezone.utc)
    end_dt   = end_dt.astimezone(timezone.utc)
    saved_at = saved_at.astimezone(timezone.utc)

    event_occurrence = EventOccurrence(
        event_id=event_id,
        org_id=org_id,
        category_id=category_id,
        title=title,
        start_datetime=start_dt,
        end_datetime=end_dt,
        event_saved_at=saved_at,
        recurrence=recurrence,
        is_all_day=is_all_day,
        user_edited=user_edited,
        description=description,
        location=location,
        source_url=source_url)
    db.add(event_occurrence)
    db.flush()
    db.refresh(event_occurrence)
    return event_occurrence


def populate_event_occurrences(db, event: Event, rule: RecurrenceRule):
    """
    Populate occurrences for a recurring event based on the recurrence rule.
    If count is set, respects the count -> No limit from until or 6-month cap.
    If count is not set and until is set, respects the until date with a 6-month cap, and stores the orig until date in the rule. (see add_recurrence_rule)
    If both count and until are not set, uses a 6-month cap from now, and stores the orig until date in the rule. (see add_recurrence_rule)
    Args:
        db: Database session.
        event: The Event object for which occurrences are to be populated.
        rule: The RecurrenceRule object defining the recurrence pattern.
    Returns:
        A message indicating the number of occurrences populated.
    """
    if event.id == TRACE_EVENT_ID:
        print("🧭 TRACE: populate_event_occurrences()")

    event_tz = ZoneInfo(event.event_timezone)
    # Defensive duration (end could be equal to start in some feeds)
    end_datetime = _parse_iso_aware(event.end_datetime, event_tz) if event.end_datetime else None
    start_datetime = _parse_iso_aware(event.start_datetime, event_tz) if event.start_datetime else None

    trace(event, "event_tz =", event_tz)

    trace(event,
        "start_datetime =", event.start_datetime,
        "end_datetime =", event.end_datetime,
        "tz =", event_tz
    )

    duration = (end_datetime or start_datetime) - start_datetime
    if duration.total_seconds() < 0:
        duration = timedelta(0)

    # Calculate time bounds
    now_utc = datetime.now(timezone.utc)
    six_months_later = now_utc + timedelta(days=180)

    # Safe copy of rule "view" for expansion window
    temp_rule = deepcopy(rule)
    if not temp_rule.count:
        if not temp_rule.until:
            temp_rule.until = six_months_later
        else:
            temp_rule.until = min(_ensure_aware(temp_rule.until), six_months_later)

    print("➡️ rule.start_datetime =", rule.start_datetime)
    # print("➡️ rule.until =", rule.until)
    # print("➡️ temp_rule.until =", temp_rule.until)

    trace(event,
        "rule.start =", rule.start_datetime,
        "rule.until =", rule.until,
        "temp.until =", temp_rule.until
    )

    # Build an rrule iterator from temp_rule
    rrule_iter = list(get_rrule_from_db_rule(temp_rule, event_tz))
    trace(event, "RRULE count =", len(rrule_iter))

    # Pull EXDATE/RDATE/Overrides/RecurrenceOverrides from DB
    exdates = {
        _ensure_aware(x.exdate) for x in db.query(RecurrenceExdate)
            .filter_by(rrule_id=rule.id).all()
    }

    rdates = {
        _ensure_aware(x.rdate) for x in db.query(RecurrenceRdate)
            .filter_by(rrule_id=rule.id).all()
    }

    overrides = {
        normalize_occurrence(_ensure_aware(o.recurrence_date), event_tz): o
        for o in db.query(EventOverride).filter_by(rrule_id=rule.id).all()
    }

    recurrence_overrides = db.query(RecurrenceOverride).filter_by(rrule_id=rule.id).all()

    # Construct a dictionary of dates: RecurrenceOverride
    recurrence_override_dates = {}
    for ro in recurrence_overrides:
        try:
            ro_rrule = rrule_from_db_recurrence_override(ro)
            for ro_date in ro_rrule:
                ro_date = normalize_occurrence(_ensure_aware(ro_date), event_tz)
                # If multiple patterns match the same date, later ones win
                # (could add priority field later if needed)
                recurrence_override_dates[ro_date] = ro
        except Exception as e:
            print(f"⚠️ Failed to expand RecurrenceOverride {ro.id}: {e}")

    # Start fresh for this event's occurrences
    deleted = db.query(EventOccurrence).filter_by(event_id=event.id).delete(synchronize_session=False)
    trace(event, "Deleted existing occurrences:", deleted)

    count = 0
    seen_starts = set()  # to avoid dupes when RDATE == RRULE date

    # 1) Generate occurrences from RRULE, skipping EXDATE and applying overrides
    exdates = normalize_set_to_tz(exdates, event_tz)
    rdates  = normalize_set_to_tz(rdates, event_tz)

    for occ_start in rrule_iter:
        occ_start = normalize_occurrence(occ_start, event_tz)

        if event.id == TRACE_EVENT_ID and count < 3:
            print("🧭 TRACE: occ_start =", occ_start)

        if occ_start in exdates:
            continue

        start_dt, end_dt, title, desc, loc = apply_overrides(
            occ_start, event, duration, overrides, recurrence_override_dates
        )

        start_dt_utc = start_dt.astimezone(timezone.utc)
        end_dt_utc   = end_dt.astimezone(timezone.utc)

        db.add(EventOccurrence(
            event_id=event.id,
            org_id=event.org_id,
            category_id=event.category_id,
            title=title,
            start_datetime=start_dt_utc,
            end_datetime=end_dt_utc,
            event_saved_at=event.last_updated_at,
            recurrence="RECURRING",
            is_all_day=event.is_all_day,
            user_edited=event.user_edited,
            description=desc,
            location=loc,
            source_url=event.source_url,
        ))

        seen_starts.add(start_dt.astimezone(timezone.utc))
        count += 1

    # 2) Add RDATEs that weren't already covered
    for rdate in sorted(rdates):
        if rdate in exdates or rdate.astimezone(timezone.utc) in seen_starts:
            continue

        start_dt, end_dt, title, desc, loc = apply_overrides(
            rdate, event, duration, overrides, recurrence_override_dates
        )

        # Respect the same 6-month cap when no count/until
        if not rule.count and (start_dt > six_months_later):
            continue

        start_dt_utc = start_dt.astimezone(timezone.utc)
        end_dt_utc   = end_dt.astimezone(timezone.utc)

        db.add(EventOccurrence(
            event_id=event.id,
            org_id=event.org_id,
            category_id=event.category_id,
            title=title,
            start_datetime= start_dt_utc,
            end_datetime=end_dt_utc,
            event_saved_at=event.last_updated_at,
            recurrence="RECURRING",
            is_all_day=event.is_all_day,
            user_edited=event.user_edited,
            description=desc,
            location=loc,
            source_url=event.source_url,
        ))

        count += 1

    # Mark successful regeneration
    now = datetime.now(timezone.utc)
    rule.last_generated_at = now
    event.last_updated_at = now

    db.flush()
    trace(event,
        "Occurrences in session =",
        db.query(EventOccurrence).filter_by(event_id=event.id).count()
    )
    return f"Populated {count} occurrences for event {event.id}"

def update_event_occurrence(db, event_id: int, org_id: int, category_id: int, title: str, 
                          start_datetime, end_datetime, recurrence: RecurrenceType,
                          event_saved_at: str, 
                          is_all_day: bool, user_edited: List[int], description: str = None, 
                          location: str = None, source_url: str = None):
    """
    Update an event occurrence in the database.

    Args:
        db: Database session.
        event_id: ID of the event.
        start_time: Start time of the occurrence in ISO format.
        end_time: End time of the occurrence in ISO format.

    Returns:
        The created EventOccurrence object.
    """
    existing_occurrence = db.query(EventOccurrence).filter_by(
        event_id=event.id,
        start_datetime=start_datetime  # or some unique combination
    ).first()

    start_dt = _parse_iso_aware(start_datetime) if isinstance(start_datetime, str) else start_datetime
    end_dt   = _parse_iso_aware(end_datetime)   if isinstance(end_datetime, str)   else end_datetime
    saved_at = _parse_iso_aware(event_saved_at) if isinstance(event_saved_at, str) else event_saved_at
    
    if existing_occurrence:
        # Update fields in-place
        existing_occurrence.title = title
        existing_occurrence.end_datetime = end_dt
        existing_occurrence.event_saved_at = saved_at
        existing_occurrence.recurrence = recurrence
        existing_occurrence.is_all_day = is_all_day
        existing_occurrence.user_edited = user_edited
        existing_occurrence.description = description
        existing_occurrence.location = location
        existing_occurrence.source_url = source_url
        db.flush()
        db.refresh(existing_occurrence)
        return existing_occurrence
    else:
        event_occurrence = EventOccurrence(
            event_id=event_id,
            org_id=org_id,
            category_id=category_id,
            title=title,
            start_datetime=start_dt,
            end_datetime=end_dt,
            event_saved_at=saved_at,
            recurrence=recurrence,
            is_all_day=is_all_day,
            user_edited=user_edited,
            description=description,
            location=location,
            source_url=source_url)
        db.add(event_occurrence)
        db.flush()
        db.refresh(event_occurrence)
        return event_occurrence

def as_dict(self):
    result = {}
    for c in self.__table__.columns:
        value = getattr(self, c.name)
        if isinstance(value, enum.Enum):   # catches RecurrenceType
            value = value.value            # or value.name if you prefer
        result[c.name] = value
    return result
def regenerate_event_occurrences_by_event_ids(db, event_ids: List[int]) -> Dict[int, str]:
    """
    Regenerate occurrences for a list of event IDs.

    Args:
        db: Database session.
        event_ids: List of event IDs to regenerate occurrences for.
    Returns:
        number of occurrences regenerated, skipped
    """
    regenerated = 0
    skipped = 0
    start = datetime.now(timezone.utc)
    for event_id in event_ids:
        event = db.query(Event).get(event_id)

        if event_id == TRACE_EVENT_ID:
            print("🧭 TRACE: Found event", event_id)

        if not event:
            skipped += 1
            continue

        rule = db.query(RecurrenceRule).filter_by(event_id=event.id).first()

        if event_id == TRACE_EVENT_ID:
            print("🧭 TRACE: Rule exists?", bool(rule))

        if not rule:
            skipped += 1
            continue

        if rule.last_generated_at and rule.last_generated_at > event.last_updated_at:
            if event_id == TRACE_EVENT_ID:
                print("🧭 TRACE: SKIPPED due to timestamps", rule.last_generated_at, event.last_updated_at)
            continue

        try:
            populate_event_occurrences(db, event, rule)
        except Exception as e:
            print("FAILED during populate:", e)
            raise
        regenerated += 1
    end = datetime.now(timezone.utc)
    # total minutes
    total_time = (end - start).total_seconds() / 60
    print(f"Regenerated occurrences for {regenerated} events, skipped {skipped} events in {total_time} minutes.")
    return regenerated, skipped
