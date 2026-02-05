from icalendar import Calendar

from app.models.calendar_source import deactivate_calendar_source
from app.utils.date import _ensure_aware, _parse_iso, decoded_dt_with_tz, infer_semester_from_datetime, parsed_httpdate_to_dt
from zoneinfo import ZoneInfo
from sqlalchemy import select, delete

from datetime import datetime, timedelta, timezone, date
from typing import Dict, List, Optional
import requests
from requests.exceptions import HTTPError, Timeout, ConnectionError
from app.errors.ical import ICalFetchError
import os
from sqlalchemy.orm import Session
import hashlib

from app.models.models import (
    Event, RecurrenceRule, EventOccurrence,
    RecurrenceExdate, RecurrenceRdate, EventOverride, CalendarSource
)

from app.models.recurrence_rule import add_recurrence_rule
from app.models.event_occurrence import populate_event_occurrences, save_event_occurrence
from app.models.event import save_event

LOOKAHEAD_DAYS = 180  # window for generating occurrences

def normalize_ics_datetime(dt, calendar_tz: ZoneInfo):
    """
    Normalize datetime or date parsed from ICS.

    Rules:
    - If dt is a DATE (all-day): treat as midnight in calendar_tz
    - If dt is a floating datetime: interpret in calendar_tz
    - If dt has tzinfo: trust it (do NOT re-localize)
    - Do NOT convert to UTC here
    """
    if isinstance(dt, date) and not isinstance(dt, datetime):
        # All-day date → midnight local time
        return datetime(dt.year, dt.month, dt.day, tzinfo=calendar_tz)

    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=calendar_tz)
        return dt

    raise TypeError(f"Unsupported ICS datetime type: {type(dt)}")

def get_calendar_timezone(cal: Calendar) -> Optional[ZoneInfo]:
    """
    Derive calendar timezone from an ICS Calendar object.
    Should work for Google Calendar, Apple Calendar, Outlook, most RFC-compliant ICS feeds.

    Priority:
    1. X-WR-TIMEZONE (Google, Apple)
    2. VTIMEZONE TZID
    """

    # X-WR-TIMEZONE (most common for Google Calendar)
    tzname = cal.get("X-WR-TIMEZONE")
    if tzname:
        try:
            return ZoneInfo(str(tzname))
        except Exception:
            pass

    # VTIMEZONE component
    for component in cal.walk("VTIMEZONE"):
        tzid = component.get("TZID")
        if tzid:
            try:
                return ZoneInfo(str(tzid))
            except Exception:
                pass

    return None

def import_ical_feed_using_helpers(
    db_session,
    ical_text_or_url: str,
    *,
    org_id: int,
    category_id: int,
    calendar_source_id: int,
    semester: Optional[str] = None,
    default_event_type: Optional[str] = None,   # e.g. "CLUB"/"ACADEMIC"/"CAREER"/"OH"/NONE
    source_url: Optional[str] = None,
    # user_edited: Optional[List[int]] = None,
    user_id: Optional[int] = None,
    delete_missing_uids: bool = False           # if True, remove events that disappeared from feed
):
    """
    Parse an ICS (string or URL), group by UID, and upsert events using the same logic
    as /create_event. All datetimes passed to helpers as ISO strings.
    """
    # 1) Load ICS (support webcal:// or https:// or raw text)
    ical_text = _fetch_ics_text(ical_text_or_url)

    try:
        cal = Calendar.from_ical(ical_text)
    except Exception:
        return {
            "success": False,
            "error": "ICAL_PARSE_ERROR",
            "message": "The calendar data could not be parsed as a valid iCal file.",
        }
    
    calendar_tz = (
        get_calendar_timezone(cal)
        or ZoneInfo("UTC")  # last-resort fallback
    )

    event_ids = []

    # 2) Group VEVENTs by UID
    by_uid: Dict[str, List] = {}
    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue
        uid = str(comp.get("UID") or "").strip()
        if not uid:
            continue
        by_uid.setdefault(uid, []).append(comp)

    incoming_uids = set(by_uid.keys())

    now = datetime.now(timezone.utc)
    horizon = now + timedelta(days=LOOKAHEAD_DAYS)

    # 3) Process each UID group
    for uid, components in by_uid.items():
        event_id = _process_uid_group_with_helpers(
            db_session=db_session,
            uid=uid,
            calendar_tz=calendar_tz,
            components=components,
            now=now,
            horizon=horizon,
            org_id=org_id,
            category_id=category_id,
            default_event_type=default_event_type,
            source_url=source_url,
            user_id=user_id,
            semester=semester,
            calendar_source_id=calendar_source_id
        )
        if event_id:
            event_ids.append(event_id)

    # 4) Optionally delete events no longer present
    if delete_missing_uids:
        existing_uids = {row[0] for row in db_session.query(Event.ical_uid).filter(Event.calendar_source_id == calendar_source_id).all()}
        missing = list(existing_uids - incoming_uids)
        if missing:
            db_session.query(Event).filter(Event.ical_uid.in_(missing)).delete(synchronize_session=False)

    return {
        "success": True,
        "event_ids": event_ids,
    }

def sync_ical_source(db: Session, source_id: int) -> str:
    now = datetime.now(timezone.utc)

    # 1️⃣ Acquire lock atomically
    source = (
        db.execute(
            select(CalendarSource)
            .where(CalendarSource.id == source_id)
            .with_for_update()
        )
        .scalar_one()
    )

    if source.locked_at and (now - source.locked_at).total_seconds() < 1800:
        return 'locked'

    source.locked_at = now
    source.lock_owner = os.getenv('HOSTNAME', 'worker')
    source.updated_at = now
    db.flush()

    try:
        # 2️⃣ Fetch ICS with caching headers
        url = source.url.replace('webcal://', 'https://')
        headers = {}

        if source.etag:
            headers['If-None-Match'] = source.etag
        if source.last_modified_hdr:
            headers['If-Modified-Since'] = source.last_modified_hdr.strftime(
                '%a, %d %b %Y %H:%M:%S GMT'
            )

        resp = requests.get(url, headers=headers, timeout=30)

        if resp.status_code == 304:
            source.last_sync_status = 'not_modified'
            source.last_fetched_at = now
            source.next_due_at = now + timedelta(seconds=source.fetch_interval_seconds)
            source.updated_at = now
            db.flush()
            return 'not_modified'

        resp.raise_for_status()
        body = resp.text

        # 3️⃣ Delta check (hash)
        body_hash = hashlib.sha256(body.encode('utf-8')).hexdigest()
        if source.sync_mode == 'delta' and body_hash == source.content_hash:
            source.last_sync_status = 'not_modified'
            source.last_fetched_at = now
            source.next_due_at = now + timedelta(seconds=source.fetch_interval_seconds)
            source.updated_at = now
            db.flush()
            return 'not_modified'

        # 4️⃣ Import with savepoint
        with db.begin_nested():
            result = import_ical_feed_using_helpers(
                db_session=db,
                ical_text_or_url=body,
                calendar_source_id=source.id,
                org_id=source.org_id,
                category_id=source.category_id,
                default_event_type=source.default_event_type,
                source_url=source.url,
                delete_missing_uids=(source.deletion_policy == 'mirror'),
            )

            if not result.get('success'):
                raise RuntimeError(result.get('error', 'ICAL_IMPORT_FAILED'))

        # 5️⃣ Update metadata
        source.content_hash = body_hash
        source.etag = resp.headers.get('ETag') or source.etag

        lm = resp.headers.get('Last-Modified')
        if lm:
            source.last_modified_hdr = parsed_httpdate_to_dt(lm)

        source.last_sync_status = 'ok'
        source.last_fetched_at = now
        source.next_due_at = now + timedelta(seconds=source.fetch_interval_seconds)
        source.updated_at = now

        db.flush()
        return 'ok'

    except Exception as e:
        source.last_error = str(e)[:500]
        source.last_sync_status = 'error'
        source.updated_at = now
        db.flush()
        raise

    finally:
        # 6️⃣ Always release lock
        source.locked_at = None
        source.lock_owner = None
        source.updated_at = datetime.now(timezone.utc)
        db.flush()



def _process_uid_group_with_helpers(
    *,
    db_session,
    uid: str,
    calendar_tz: ZoneInfo,
    components: List,
    now: datetime,
    horizon: datetime,
    calendar_source_id: int,
    org_id: int,
    category_id: int,
    default_event_type: Optional[str],
    source_url: Optional[str],
    user_id: Optional[int],
    semester: Optional[str],
):
    # Split: base components (no RECURRENCE-ID) vs overrides
    base_candidates = [c for c in components if not c.get("RECURRENCE-ID")]
    override_components = [c for c in components if c.get("RECURRENCE-ID")]

    if not base_candidates:
        return

    base = _pick_base_component(base_candidates)
    dtstart = normalize_ics_datetime(decoded_dt_with_tz(base, "DTSTART"), calendar_tz).astimezone(timezone.utc)

    event_semester = semester or infer_semester_from_datetime(dtstart)

    min_dt = now - timedelta(days=365)
    if dtstart < min_dt:
        # skip events older than 1 year
        return None


    # Base fields
    raw_dtstart = decoded_dt_with_tz(base, "DTSTART")
    raw_dtend   = decoded_dt_with_tz(base, "DTEND")
    dtstart = normalize_ics_datetime(raw_dtstart, calendar_tz)
    dtend   = normalize_ics_datetime(raw_dtend, calendar_tz) if raw_dtend else None

    is_all_day = _is_all_day_component(base)

    # Convert to ISO strings for your helper
    start_iso = _to_iso_for_helper(dtstart, is_all_day)
    end_iso   = _to_iso_for_helper(dtend,   is_all_day) if dtend else start_iso

    title = str(base.get("SUMMARY") or "").strip()
    description = str(base.get("DESCRIPTION") or "").strip()
    location = str(base.get("LOCATION") or "no location recorded").strip()

    seq = int(base.get("SEQUENCE", 0) or 0)
    lm  = base.get("LAST-MODIFIED")
    last_modified = lm.dt if lm else None

    # --- DEDUPE LOOKUP (scope by org+category; add source_id if you have it) ---
    existing = (
        db_session.query(Event)
        .filter(
            Event.calendar_source_id == calendar_source_id,
            Event.ical_uid == uid,
        )
        .first()
    )
    legacy = None
    if not existing:
        # Legacy adoption (backfill)
        start_utc = normalize_ics_datetime(dtstart, calendar_tz).astimezone(timezone.utc)
        end_utc = normalize_ics_datetime(dtend, calendar_tz).astimezone(timezone.utc) if dtend else None
        
        legacy = (
            db_session.query(Event)
            .filter(
                Event.calendar_source_id.is_(None),
                Event.ical_uid.is_(None),
                Event.org_id == org_id,
                Event.title == title,
                Event.start_datetime == start_utc,
                Event.end_datetime == end_utc,
                Event.location == location,
            )
            .first()
        )
    event_row = existing or legacy
    adopted = legacy is not None

    changed = _should_update(event_row, seq, last_modified, adopted)

    # Upsert the Event by UID (using your helper flow)
    # We mirror the /create_event argument structure and then set iCal metadata after flush.
    # existing = db_session.query(Event).filter_by(ical_uid=uid).first()
    if adopted:
        event_row.calendar_source_id = calendar_source_id
        event_row.ical_uid = uid
        db_session.flush()

    if event_row:
        # Decide if we should update (SEQUENCE or LAST-MODIFIED newer)
        if changed:
            
            event_row.title = title
            event_row.description = description or ""
            event_row.location = location or "no location recorded"
            event_row.start_datetime = _ensure_aware(dtstart)
            event_row.end_datetime = _ensure_aware(dtend) if dtend else _ensure_aware(dtstart)
            event_row.is_all_day = is_all_day
            event_row.event_timezone = str(calendar_tz)
            event_row.source_url = source_url
            event_row.event_type = default_event_type
            event_row.semester = event_semester
            event_row.calendar_source_id = calendar_source_id

            user_edited = event_row.user_edited if event_row.user_edited else []
            user_edited.append(user_id)
            event_row.user_edited = user_edited

            event_row.org_id = org_id
            event_row.category_id = category_id
            event_row.calendar_source_id = calendar_source_id
            event_row.ical_uid = uid
            event_row.ical_sequence = seq
            event_row.ical_last_modified = _ensure_aware(last_modified) if last_modified else None
            db_session.flush()
            event = event_row
        else:
            print(f"No changes detected for event {event_row.id}")
            # will skip the rest of the steps for this event.
            return event_row.id
    else:
        # Create via your helper (expects ISO strings)
        event = save_event(
            db_session,
            org_id=org_id,
            category_id=category_id,
            title=title,
            description=description or None,
            start_datetime=start_iso,
            end_datetime=end_iso,
            is_all_day=is_all_day,
            event_timezone=str(calendar_tz),
            location=location or None,
            source_url=source_url,
            event_type=default_event_type,
            semester=event_semester,
            user_edited=[user_id],
            calendar_source_id=calendar_source_id,
            ical_uid=uid,
            ical_sequence=seq,
            ical_last_modified=_ensure_aware(last_modified) if last_modified else None,
        )
        db_session.flush()
        db_session.flush()

    # Handle recurrence rule (RRULE) + EXDATE + RDATE
    rrule = base.get("RRULE")
    rule: Optional[RecurrenceRule] = None

    if rrule:
        # Prepare recurrence_data for your helper
        # NOTE: all datetimes passed as ISO
        until_val = (rrule.get("UNTIL") or [None])[0]
        # icalendar may give UNTIL as date/datetime; convert to ISO string if present
        if until_val:
            until_dt = normalize_ics_datetime(until_val, calendar_tz)
            until_iso = until_dt.isoformat()
        else:
            until_iso = None
        by_day = rrule.get("BYDAY", [])
        if isinstance(by_day, str):
            by_day = [by_day]  # Wrap in list if single string

        recurrence_data = {
            "frequency": (rrule.get("FREQ") or [None])[0],
            "interval":  (rrule.get("INTERVAL") or [1])[0],
            "start_datetime": start_iso,
            "count":     (rrule.get("COUNT") or [None])[0],
            "until":     until_iso,
            "by_day":    by_day,
            "by_month_day": (rrule.get("BYMONTHDAY") or [None])[0],
            "by_month":     (rrule.get("BYMONTH") or [None])[0],
        }

        # Upsert rule using your helper
        existing_rule = db_session.query(RecurrenceRule).filter_by(event_id=event.id).first()
        if existing_rule:
            # Update in place to mirror add_recurrence_rule behavior
            existing_rule.frequency = recurrence_data["frequency"]
            existing_rule.interval = recurrence_data["interval"]
            existing_rule.count = recurrence_data["count"]
            existing_rule.until = _parse_iso(until_iso) if until_iso else None
            existing_rule.by_day = recurrence_data["by_day"] or None
            existing_rule.by_month_day = recurrence_data["by_month_day"]
            existing_rule.by_month = recurrence_data["by_month"]
            existing_rule.start_datetime = _parse_iso(start_iso)
            db_session.flush()
            rule = existing_rule
        else:
            rule = add_recurrence_rule(
                db_session,
                event_id=event.id,
                frequency=recurrence_data["frequency"],
                interval=recurrence_data["interval"],
                start_datetime=recurrence_data["start_datetime"],  # ISO
                count=recurrence_data["count"],
                until=recurrence_data["until"],                    # ISO or None
                by_day=recurrence_data["by_day"],
                by_month_day=recurrence_data["by_month_day"],
                by_month=recurrence_data["by_month"],
            )
            db_session.flush()

        # Refresh EXDATEs / RDATEs idempotently
        db_session.query(RecurrenceExdate).filter_by(rrule_id=rule.id).delete(synchronize_session=False)
        db_session.query(RecurrenceRdate).filter_by(rrule_id=rule.id).delete(synchronize_session=False)

        # ---- Safe EXDATE normalization ----
        raw_exdates = base.get("EXDATE")

        exdate_entries = []
        if raw_exdates:
            from icalendar.prop import vDDDLists

            # Case A: single vDDDLists → wrap into list
            if isinstance(raw_exdates, vDDDLists):
                exdate_entries = [raw_exdates]
            else:
                # Case B: already a list, but items may or may not be vDDDLists
                # Filter or wrap appropriately
                for item in raw_exdates:
                    if isinstance(item, vDDDLists):
                        exdate_entries.append(item)
                    else:
                        # A single EXDATE entry written as literal ical datetime
                        # Wrap into vDDDLists-like object
                        exdate_entries.append(item)

        # Iterate safely
        for ex in exdate_entries:
            for ex_date in ex.dts:
                ex_dt = normalize_ics_datetime(ex_date.dt, calendar_tz)
                db_session.add(RecurrenceExdate(
                    rrule_id=rule.id,
                    exdate=ex_dt.astimezone(timezone.utc)
                ))

        # ---- Safe RDATE normalization ----
        raw_rdates = base.get("RDATE")

        rdate_entries = []
        if raw_rdates:
            from icalendar.prop import vDDDLists

            if isinstance(raw_rdates, vDDDLists):
                rdate_entries = [raw_rdates]
            else:
                for item in raw_rdates:
                    if isinstance(item, vDDDLists):
                        rdate_entries.append(item)
                    else:
                        rdate_entries.append(item)

        for entry in rdate_entries:
            for rd in entry.dts:
                db_session.add(RecurrenceRdate(
                    rrule_id=rule.id,
                    rdate=normalize_ics_datetime(rd.dt, calendar_tz).astimezone(timezone.utc)
                ))
                db_session.flush()

        # Store overrides for this UID (RECURRENCE-ID)
        db_session.query(EventOverride).filter_by(rrule_id=rule.id).delete(synchronize_session=False)
        for oc in override_components:
            rid = oc.get("RECURRENCE-ID")
            if not rid:
                continue
            rid_dt = normalize_ics_datetime(rid.dt, calendar_tz)
            db_session.add(EventOverride(
                rrule_id=rule.id,
                recurrence_date=rid_dt.astimezone(timezone.utc),
                new_start=decoded_dt_with_tz(oc, "DTSTART"),
                new_end=decoded_dt_with_tz(oc, "DTEND"),
                new_title=str(oc.get("SUMMARY") or None),
                new_description=str(oc.get("DESCRIPTION") or None),
                new_location=str(oc.get("LOCATION") or None),
            ))
        db_session.flush()

        # Regenerate occurrences
        # (populate_event_occurrences reads rule + exdates/rdates/overrides)
        populate_event_occurrences(db_session, event=event, rule=rule)

    else:
        # One-time event: clean any previous rule + just write one occurrence
        old_rule = db_session.query(RecurrenceRule).filter_by(event_id=event.id).first()
        if old_rule and changed:
            db_session.query(RecurrenceExdate).filter_by(rrule_id=old_rule.id).delete(synchronize_session=False)
            db_session.query(RecurrenceRdate).filter_by(rrule_id=old_rule.id).delete(synchronize_session=False)
            # since we set ON DELETE CASCADE for overrides linked to rule, this single delete is enough:
            db_session.delete(old_rule)
            db_session.flush()
        
        if changed or not _has_occurrence(
            db_session,
            event.id,
            normalize_ics_datetime(dtstart, calendar_tz).astimezone(timezone.utc),
            normalize_ics_datetime(dtend, calendar_tz).astimezone(timezone.utc) if dtend else None
        ):

        # Write a single occurrence via your helper
            event_saved_at = getattr(event, "last_updated_at", datetime.now(timezone.utc))

            save_event_occurrence(
                db_session,
                event_id=event.id,
                org_id=org_id,
                category_id=category_id,
                title=title,
                start_datetime=start_iso,  # ISO
                end_datetime=end_iso,      # ISO
                recurrence="ONETIME",
                event_saved_at=event_saved_at,
                is_all_day=is_all_day,
                event_timezone=str(calendar_tz),
                user_edited=[user_id],
                description=description or None,
                location=location or None,
                source_url=source_url
            )
    return event.id

    # Optional: if you want occurrences only up to `horizon`, keep your populate logic bounded by a horizon.


# -----------------------
# Utilities
# -----------------------

def _has_occurrence(db_session, event_id: int, start_dt, end_dt) -> bool:
    q = (db_session.query(EventOccurrence.id)
         .filter_by(event_id=event_id)
         .filter(EventOccurrence.start_datetime == start_dt))
    if end_dt is not None:
        q = q.filter(EventOccurrence.end_datetime == end_dt)
    return db_session.query(q.exists()).scalar()

def _fetch_ics_text(ical_text_or_url: str) -> str:
    s = ical_text_or_url.strip()
    # Raw ICS text
    if s.startswith("BEGIN:VCALENDAR"):
        return s
    if s.startswith(("http://", "https://", "webcal://")):
        if s.startswith("webcal://"):
            s = "https://" + s[len("webcal://"):]
        try:
            r = requests.get(s, timeout=30)
            r.raise_for_status()
            content_type = (r.headers.get("Content-Type") or "").lower()
            if "text/html" in content_type:
                raise ICalFetchError(
                    "ICAL_NOT_ICS",
                    "The provided URL does not point to a valid iCal feed.",
                )
            return r.text
        except HTTPError as e:
            status = e.response.status_code if e.response else None
            url_lower = s.lower()
            if status in (401, 403):
                raise ICalFetchError(
                    "ICAL_PERMISSION_DENIED",
                    "Access to this calendar is denied. "
                    "It may be private or require authentication.",
                )
            if status == 404:
                raise ICalFetchError(
                    "ICAL_NOT_FOUND",
                    "The iCal URL was not found. Please check the link.",
                )
            raise ICalFetchError(
                "ICAL_HTTP_ERROR",
                "Failed to fetch the iCal feed. Please verify the calendar has public access.",
            )
        except Timeout:
            raise ICalFetchError(
                "ICAL_TIMEOUT",
                "The iCal feed took too long to respond.",
            )
        except ConnectionError:
            raise ICalFetchError(
                "ICAL_CONNECTION_ERROR",
                "Unable to connect to the iCal feed.",
            )
    # Fallback: treat as raw text
    return s


def _should_update(existing_evt: Event, seq: int, last_modified: datetime, adopted_from_legacy: bool) -> bool:
        # new event
    if existing_evt is None:
        return True
    if adopted_from_legacy:
        return True
    if seq is not None and seq > (existing_evt.ical_sequence or 0):
        return True
    if last_modified and (
        existing_evt.ical_last_modified is None
        or last_modified > existing_evt.ical_last_modified
    ):
        return True
    return False

def _to_iso_for_helper(dt, is_all_day: bool) -> str:
    """
    Convert datetime to ISO string for helper. If all-day and time is midnight, keep date part normalized.
    """
    if dt is None:
        return None
    dt = _ensure_aware(dt)
    # Keep full ISO; your helpers accept ISO strings
    return dt.isoformat()

def _is_all_day_component(component) -> bool:
    # VALUE=DATE → all-day; or DTSTART was a date (handled above)
    dtstart = component.get("DTSTART")
    if not dtstart:
        return False
    params = getattr(dtstart, "params", {})
    return params.get("VALUE") == "DATE"

def _looks_like_date(val) -> bool:
    return isinstance(val, date) and not isinstance(val, datetime)

def _pick_base_component(bases: List):
    """Prefer component with RRULE; else earliest DTSTART."""
    with_rrule = [b for b in bases if b.get("RRULE")]
    if with_rrule:
        return with_rrule[0]
    return sorted(bases, key=lambda b: decoded_dt_with_tz(b, "DTSTART"))[0]

from app.models.models import (
    Event,
    CalendarSource,
    EventOccurrence,
    EventTag,
    UserSavedEvent,
    Academic,
    Career,
    Club,
    RecurrenceRule,
    RecurrenceExdate,
    RecurrenceRdate,
    EventOverride,
    RecurrenceOverride,
)


def delete_events_for_calendar_source(
    db: Session,
    calendar_source_id: int,
) -> list[int]:
    """
    Delete all events associated with a CalendarSource, including
    all dependent rows, and deactivate the CalendarSource.

    Returns:
        List of deleted event IDs.
    """

    # Lock and fetch calendar source
    calendar_source = (
        db.query(CalendarSource)
        .filter(CalendarSource.id == calendar_source_id)
        .with_for_update()
        .one_or_none()
    )

    if not calendar_source:
        raise ValueError("CalendarSource not found")

    # Collect event IDs once
    event_id_list = (
        db.query(Event.id)
        .filter(Event.calendar_source_id == calendar_source_id)
        .all()
    )
    event_id_list = [eid for (eid,) in event_id_list]

    if not event_id_list:
        # Still deactivate calendar source
        calendar_source.active = False
        calendar_source.updated_at = datetime.now(timezone.utc)
        db.flush()
        return []

    event_id_subq = select(Event.id).where(Event.id.in_(event_id_list))

    # Delete Event-level children
    db.execute(
        delete(EventOccurrence).where(EventOccurrence.event_id.in_(event_id_subq))
    )
    db.execute(
        delete(EventTag).where(EventTag.event_id.in_(event_id_subq))
    )
    db.execute(
        delete(UserSavedEvent).where(UserSavedEvent.event_id.in_(event_id_subq))
    )
    db.execute(
        delete(Academic).where(Academic.event_id.in_(event_id_subq))
    )
    db.execute(
        delete(Career).where(Career.event_id.in_(event_id_subq))
    )
    db.execute(
        delete(Club).where(Club.event_id.in_(event_id_subq))
    )

    # Handle recurrence hierarchy
    rrule_ids = (
        select(RecurrenceRule.id)
        .where(RecurrenceRule.event_id.in_(event_id_subq))
    )

    db.execute(
        delete(RecurrenceExdate).where(
            RecurrenceExdate.rrule_id.in_(rrule_ids)
        )
    )
    db.execute(
        delete(RecurrenceRdate).where(
            RecurrenceRdate.rrule_id.in_(rrule_ids)
        )
    )
    db.execute(
        delete(EventOverride).where(
            EventOverride.rrule_id.in_(rrule_ids)
        )
    )
    db.execute(
        delete(RecurrenceOverride).where(
            RecurrenceOverride.rrule_id.in_(rrule_ids)
        )
    )
    db.execute(
        delete(RecurrenceRule).where(
            RecurrenceRule.id.in_(rrule_ids)
        )
    )

    # Delete events
    db.execute(
        delete(Event).where(Event.id.in_(event_id_subq))
    )

    # Deactivate calendar source
    deactivate_calendar_source(db, calendar_source_id)

    db.flush()
    return event_id_list
