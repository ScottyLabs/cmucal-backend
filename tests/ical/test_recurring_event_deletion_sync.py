"""
Comprehensive tests for recurring Google Calendar event deletion sync.

Targets the specific bug where deleting an instance from a recurring event
on a synced Google Calendar does not properly sync (the deleted occurrence
still appears on the frontend).

Google Calendar uses two mechanisms to communicate deleted recurring instances:
  1. EXDATE entries on the base VEVENT (explicit date exclusions)
  2. STATUS:CANCELLED on a VEVENT with RECURRENCE-ID (cancelled instance)

These tests verify both paths and guard against regressions.
"""

import pytest
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.models.models import (
    Event,
    EventOccurrence,
    RecurrenceRule,
    RecurrenceExdate,
    RecurrenceRdate,
    EventOverride,
)
from app.models.calendar_source import create_calendar_source
from app.services.ical import import_ical_feed_using_helpers
from app.models.event_occurrence import populate_event_occurrences


# ---------------------------------------------------------------------------
# Helper to build ICS timestamps relative to "now" so tests don't go stale
# ---------------------------------------------------------------------------
ET = ZoneInfo("America/New_York")

def _ts(dt: datetime) -> str:
    """Format a tz-aware datetime as an ICS DATETIME string (UTC)."""
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y%m%dT%H%M%SZ")


def _date(dt: datetime) -> str:
    """Format as ICS DATE-only string."""
    return dt.strftime("%Y%m%d")


# A Monday in the near future, used as the anchor for recurring series.
_BASE = datetime(2026, 3, 2, 10, 0, tzinfo=ET)  # Monday 10 AM ET
DTSTAMP = _ts(datetime.now(timezone.utc))


# ============================================================================
# FIXTURE: scaffold org / category / user / calendar_source
# ============================================================================
@pytest.fixture
def scaffold(db, org_factory, category_factory, user_factory, calendar_source_factory):
    """Create the minimal DB scaffolding needed for an iCal import."""
    org = org_factory()
    cat = category_factory(org_id=org.id)
    user = user_factory()
    source = calendar_source_factory(
        org_id=org.id,
        category_id=cat.id,
        created_by_user_id=user.id,
    )
    return {
        "db": db,
        "org": org,
        "category": cat,
        "user": user,
        "source": source,
    }


def _import(scaffold, ics_text, delete_missing=False):
    """Convenience wrapper around import_ical_feed_using_helpers."""
    return import_ical_feed_using_helpers(
        db_session=scaffold["db"],
        ical_text_or_url=ics_text,
        calendar_source_id=scaffold["source"].id,
        org_id=scaffold["org"].id,
        category_id=scaffold["category"].id,
        user_id=scaffold["user"].id,
        source_url=scaffold["source"].url,
        delete_missing_uids=delete_missing,
    )


def _occurrence_starts(db, event_id):
    """Return sorted list of occurrence start datetimes (UTC) for an event."""
    occs = (
        db.query(EventOccurrence.start_datetime)
        .filter(EventOccurrence.event_id == event_id)
        .order_by(EventOccurrence.start_datetime)
        .all()
    )
    return [row[0] for row in occs]


# ============================================================================
# 1. BASELINE: weekly recurring event with 5 occurrences, no deletions
# ============================================================================
class TestRecurringBaselineSanity:
    """Verify that a simple recurring event generates the expected occurrences."""

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:baseline-weekly-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
SUMMARY:Weekly Standup
LOCATION:GHC 4401
END:VEVENT
END:VCALENDAR
"""

    def test_five_weekly_occurrences_created(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 5, f"Expected 5 occurrences, got {len(starts)}"

        # Each occurrence should be 7 days apart in the event's local timezone.
        # (UTC gaps vary when crossing DST, so we check in local time.)
        event = db.query(Event).get(event_id)
        event_tz = ZoneInfo(event.event_timezone)
        starts_local = [s.astimezone(event_tz) for s in starts]
        for i in range(1, len(starts_local)):
            delta = starts_local[i] - starts_local[i - 1]
            assert delta == timedelta(days=7), f"Gap {i} (local): {delta}"


# ============================================================================
# 2. EXDATE: single deletion via EXDATE property
# ============================================================================
class TestExdateSingleDeletion:
    """
    When a user deletes a single instance from a recurring event, Google Calendar
    may add an EXDATE to the base VEVENT. The deleted occurrence should NOT appear.
    """

    # Delete the 3rd occurrence (week 3 = _BASE + 14 days)
    DELETED_DT = _BASE + timedelta(weeks=2)

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:exdate-single-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
EXDATE;TZID=America/New_York:{DELETED_DT.strftime("%Y%m%dT%H%M%S")}
SUMMARY:Team Sync
LOCATION:WEH 5415
END:VEVENT
END:VCALENDAR
"""

    def test_exdate_removes_one_occurrence(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 4, f"Expected 4 occurrences (5 - 1 EXDATE), got {len(starts)}"

        # The deleted date should not be present
        deleted_utc = self.DELETED_DT.astimezone(timezone.utc)
        assert deleted_utc not in starts, "EXDATE occurrence should not exist"

    def test_exdate_stored_in_db(self, scaffold):
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]
        rule = db.query(RecurrenceRule).filter_by(event_id=event_id).first()
        assert rule is not None

        exdates = db.query(RecurrenceExdate).filter_by(rrule_id=rule.id).all()
        assert len(exdates) == 1, "Should have exactly 1 EXDATE row"


# ============================================================================
# 3. EXDATE: multiple deletions via EXDATE property
# ============================================================================
class TestExdateMultipleDeletions:
    """Delete 2 out of 5 occurrences using multiple EXDATE entries."""

    DEL1 = _BASE + timedelta(weeks=1)  # week 2
    DEL2 = _BASE + timedelta(weeks=3)  # week 4

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:exdate-multi-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
EXDATE;TZID=America/New_York:{DEL1.strftime("%Y%m%dT%H%M%S")}
EXDATE;TZID=America/New_York:{DEL2.strftime("%Y%m%dT%H%M%S")}
SUMMARY:Design Review
LOCATION:NSH 1305
END:VEVENT
END:VCALENDAR
"""

    def test_two_exdates_remove_two_occurrences(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 3, f"Expected 3 occurrences (5 - 2 EXDATE), got {len(starts)}"

    def test_exdates_stored_correctly(self, scaffold):
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]
        rule = db.query(RecurrenceRule).filter_by(event_id=event_id).first()
        exdates = db.query(RecurrenceExdate).filter_by(rrule_id=rule.id).all()
        assert len(exdates) == 2


# ============================================================================
# 4. STATUS:CANCELLED — Google's primary deletion mechanism for recurring events
#    THIS IS THE BUG: cancelled instances still appear as occurrences
# ============================================================================
class TestStatusCancelledSingleDeletion:
    """
    When Google Calendar deletes a single instance, it sends a VEVENT with:
      - RECURRENCE-ID pointing to the original occurrence date
      - STATUS:CANCELLED

    Cancelled RECURRENCE-ID instances should be excluded from generated
    occurrences and represented as exdates.
    """

    CANCELLED_DT = _BASE + timedelta(weeks=2)  # delete week 3

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:cancelled-single-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
SUMMARY:Sprint Planning
LOCATION:GHC 4307
END:VEVENT
BEGIN:VEVENT
UID:cancelled-single-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{CANCELLED_DT.strftime("%Y%m%dT%H%M%S")}
DTSTART;TZID=America/New_York:{CANCELLED_DT.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(CANCELLED_DT + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
STATUS:CANCELLED
SUMMARY:Sprint Planning
END:VEVENT
END:VCALENDAR
"""

    def test_cancelled_instance_excluded_from_occurrences(self, scaffold):
        """The cancelled instance should NOT appear in generated occurrences."""
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 4, (
            f"Expected 4 occurrences (5 - 1 cancelled), got {len(starts)}. "
            "The STATUS:CANCELLED instance was not excluded."
        )

    def test_cancelled_instance_stored_as_exdate(self, scaffold):
        """Cancelled RECURRENCE-ID should create an EXDATE, not an override."""
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]
        rule = db.query(RecurrenceRule).filter_by(event_id=event_id).first()
        assert rule is not None

        overrides = db.query(EventOverride).filter_by(rrule_id=rule.id).all()
        assert len(overrides) == 0, "Cancelled instance should not create EventOverride"

        exdates = db.query(RecurrenceExdate).filter_by(rrule_id=rule.id).all()
        assert len(exdates) == 1, "Cancelled instance should create one RecurrenceExdate"

    def test_occurrence_count_excludes_cancelled_instance(self, scaffold):
        """Recurring occurrence count should exclude cancelled RECURRENCE-ID."""
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 4, "Cancelled occurrence should be excluded"


# ============================================================================
# 5. STATUS:CANCELLED — multiple deletions
# ============================================================================
class TestStatusCancelledMultipleDeletions:
    """Google cancels 2 of 5 instances via STATUS:CANCELLED."""

    CANCEL1 = _BASE + timedelta(weeks=1)
    CANCEL2 = _BASE + timedelta(weeks=3)

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:cancelled-multi-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
SUMMARY:Retro
LOCATION:CUC McConomy
END:VEVENT
BEGIN:VEVENT
UID:cancelled-multi-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{CANCEL1.strftime("%Y%m%dT%H%M%S")}
DTSTART;TZID=America/New_York:{CANCEL1.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(CANCEL1 + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
STATUS:CANCELLED
SUMMARY:Retro
END:VEVENT
BEGIN:VEVENT
UID:cancelled-multi-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{CANCEL2.strftime("%Y%m%dT%H%M%S")}
DTSTART;TZID=America/New_York:{CANCEL2.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(CANCEL2 + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
STATUS:CANCELLED
SUMMARY:Retro
END:VEVENT
END:VCALENDAR
"""

    def test_two_cancelled_instances_excluded(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 3, (
            f"Expected 3 occurrences (5 - 2 cancelled), got {len(starts)}"
        )

    def test_two_exdates_stored_for_cancellations(self, scaffold):
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]
        rule = db.query(RecurrenceRule).filter_by(event_id=event_id).first()

        overrides = db.query(EventOverride).filter_by(rrule_id=rule.id).all()
        assert len(overrides) == 0

        exdates = db.query(RecurrenceExdate).filter_by(rrule_id=rule.id).all()
        assert len(exdates) == 2


# ============================================================================
# 6. Mixed: some instances deleted via EXDATE, some via STATUS:CANCELLED
# ============================================================================
class TestMixedExdateAndCancelled:
    """
    Real-world Google Calendar feeds often use BOTH EXDATE and STATUS:CANCELLED.
    - EXDATE for instances deleted before any user ever modified them
    - STATUS:CANCELLED for instances that were modified then deleted
    """

    EXDATE_DT = _BASE + timedelta(weeks=1)   # week 2 deleted via EXDATE
    CANCEL_DT = _BASE + timedelta(weeks=3)   # week 4 deleted via STATUS:CANCELLED

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:mixed-del-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
EXDATE;TZID=America/New_York:{EXDATE_DT.strftime("%Y%m%dT%H%M%S")}
SUMMARY:Mixed Deletions
LOCATION:WEH 4623
END:VEVENT
BEGIN:VEVENT
UID:mixed-del-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{CANCEL_DT.strftime("%Y%m%dT%H%M%S")}
DTSTART;TZID=America/New_York:{CANCEL_DT.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(CANCEL_DT + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
STATUS:CANCELLED
SUMMARY:Mixed Deletions
END:VEVENT
END:VCALENDAR
"""

    def test_both_deletion_types_honored(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 3, (
            f"Expected 3 occurrences (5 - 1 EXDATE - 1 CANCELLED), got {len(starts)}"
        )

    def test_exdate_portion_works(self, scaffold):
        """At minimum the EXDATE deletion should work correctly."""
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        exdate_utc = self.EXDATE_DT.astimezone(timezone.utc)
        assert exdate_utc not in starts, "EXDATE occurrence should be excluded"


class TestExdateCancelledDedupNoRdate:
    """
    Regression: if the same occurrence is excluded by both EXDATE and
    RECURRENCE-ID + STATUS:CANCELLED, the import should create one
    RecurrenceExdate row (not duplicates), even when there are no RDATEs.
    """

    DUP_DT = _BASE + timedelta(weeks=2)

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:dedup-exdate-cancelled-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
EXDATE;TZID=America/New_York:{DUP_DT.strftime("%Y%m%dT%H%M%S")}
SUMMARY:Dedupe Exdate Cancelled
LOCATION:GHC 4401
END:VEVENT
BEGIN:VEVENT
UID:dedup-exdate-cancelled-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{DUP_DT.strftime("%Y%m%dT%H%M%S")}
STATUS:CANCELLED
END:VEVENT
END:VCALENDAR
"""

    def test_same_occurrence_excluded_once(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 4

    def test_no_duplicate_exdate_rows(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        rule = db.query(RecurrenceRule).filter_by(event_id=event_id).first()
        assert rule is not None

        exdates = db.query(RecurrenceExdate).filter_by(rrule_id=rule.id).all()
        matching = [
            x for x in exdates
            if x.exdate == self.DUP_DT.astimezone(timezone.utc)
        ]
        assert len(matching) == 1, "Expected one EXDATE row for duplicated exclusion date"


# ============================================================================
# 7. Override (not cancelled): modified instance should still appear
# ============================================================================
class TestRecurrenceIdModifiedNotCancelled:
    """
    A RECURRENCE-ID VEVENT that is NOT cancelled (just modified) should still
    appear in occurrences — with the updated title/time/location.
    """

    MODIFIED_DT = _BASE + timedelta(weeks=2)
    NEW_START = MODIFIED_DT.replace(hour=14)  # moved from 10 AM to 2 PM
    NEW_END = NEW_START + timedelta(hours=1)

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:override-mod-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
SUMMARY:Team Huddle
LOCATION:GHC 6115
END:VEVENT
BEGIN:VEVENT
UID:override-mod-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{MODIFIED_DT.strftime("%Y%m%dT%H%M%S")}
DTSTART;TZID=America/New_York:{NEW_START.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{NEW_END.strftime("%Y%m%dT%H%M%S")}
SUMMARY:Team Huddle (Rescheduled)
LOCATION:CUC Rangos
END:VEVENT
END:VCALENDAR
"""

    def test_modified_instance_still_appears(self, scaffold):
        """All 5 occurrences should exist — one with modified data."""
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 5, f"Expected 5 occurrences, got {len(starts)}"

    def test_modified_instance_has_updated_title(self, scaffold):
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        # Find the overridden occurrence (the one with different title)
        overridden = (
            db.query(EventOccurrence)
            .filter(
                EventOccurrence.event_id == event_id,
                EventOccurrence.title == "Team Huddle (Rescheduled)",
            )
            .all()
        )
        assert len(overridden) == 1, "Should have exactly 1 overridden occurrence"

    def test_modified_instance_has_updated_time(self, scaffold):
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        new_start_utc = self.NEW_START.astimezone(timezone.utc)
        matching = (
            db.query(EventOccurrence)
            .filter(
                EventOccurrence.event_id == event_id,
                EventOccurrence.start_datetime == new_start_utc,
            )
            .all()
        )
        assert len(matching) == 1, (
            f"Expected 1 occurrence at {new_start_utc}, found {len(matching)}"
        )


# ============================================================================
# 8. Re-import after deletion: EXDATE added on second import
# ============================================================================
class TestReimportWithNewExdate:
    """
    Simulate: user initially has 5 occurrences, then deletes one on Google Calendar.
    The next iCal sync brings the same feed with an EXDATE added. The occurrence
    should be removed.
    """

    ICS_BEFORE = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:reimport-exdate-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
SEQUENCE:0
SUMMARY:Sprint Demo
LOCATION:WEH 7500
END:VEVENT
END:VCALENDAR
"""

    DELETED_DT = _BASE + timedelta(weeks=2)

    ICS_AFTER = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:reimport-exdate-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
EXDATE;TZID=America/New_York:{DELETED_DT.strftime("%Y%m%dT%H%M%S")}
SEQUENCE:1
SUMMARY:Sprint Demo
LOCATION:WEH 7500
END:VEVENT
END:VCALENDAR
"""

    def test_reimport_removes_deleted_occurrence(self, scaffold):
        # First import: 5 occurrences
        result1 = _import(scaffold, self.ICS_BEFORE)
        assert result1["success"]
        db = scaffold["db"]
        event_id = result1["event_ids"][0]
        assert len(_occurrence_starts(db, event_id)) == 5

        # Second import: EXDATE added, should now have 4
        result2 = _import(scaffold, self.ICS_AFTER)
        assert result2["success"]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 4, (
            f"After re-import with EXDATE, expected 4 occurrences, got {len(starts)}"
        )

    def test_reimport_does_not_create_duplicate_event(self, scaffold):
        result1 = _import(scaffold, self.ICS_BEFORE)
        result2 = _import(scaffold, self.ICS_AFTER)
        assert result1["event_ids"] == result2["event_ids"], "Should reuse same event ID"


# ============================================================================
# 9. Re-import after deletion via STATUS:CANCELLED (second sync)
# ============================================================================
class TestReimportWithNewCancellation:
    """
    First sync: 5 occurrences. Second sync: one instance gets STATUS:CANCELLED.
    The cancelled occurrence should be removed.
    """

    ICS_BEFORE = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:reimport-cancel-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
SEQUENCE:0
SUMMARY:Code Review
LOCATION:CIC 2000
END:VEVENT
END:VCALENDAR
"""

    CANCEL_DT = _BASE + timedelta(weeks=2)

    ICS_AFTER = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:reimport-cancel-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
SEQUENCE:1
SUMMARY:Code Review
LOCATION:CIC 2000
END:VEVENT
BEGIN:VEVENT
UID:reimport-cancel-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{CANCEL_DT.strftime("%Y%m%dT%H%M%S")}
DTSTART;TZID=America/New_York:{CANCEL_DT.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(CANCEL_DT + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
STATUS:CANCELLED
SUMMARY:Code Review
SEQUENCE:1
END:VEVENT
END:VCALENDAR
"""

    def test_reimport_with_cancellation_removes_occurrence(self, scaffold):
        result1 = _import(scaffold, self.ICS_BEFORE)
        assert result1["success"]
        db = scaffold["db"]
        event_id = result1["event_ids"][0]
        assert len(_occurrence_starts(db, event_id)) == 5

        result2 = _import(scaffold, self.ICS_AFTER)
        assert result2["success"]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 4, (
            f"After re-import with STATUS:CANCELLED, expected 4, got {len(starts)}"
        )


# ============================================================================
# 10. Daily recurring event with EXDATE
# ============================================================================
class TestDailyRecurrenceWithExdate:
    """Ensure EXDATE works for daily recurrences too, not just weekly."""

    DAILY_BASE = _BASE
    DELETED_DT = _BASE + timedelta(days=3)

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:daily-exdate-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{DAILY_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(DAILY_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=DAILY;COUNT=7
EXDATE;TZID=America/New_York:{DELETED_DT.strftime("%Y%m%dT%H%M%S")}
SUMMARY:Daily Check-in
LOCATION:Zoom
END:VEVENT
END:VCALENDAR
"""

    def test_daily_exdate_removes_occurrence(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 6, f"Expected 6 (7 - 1 EXDATE), got {len(starts)}"


# ============================================================================
# 11. All-day recurring event with a deleted instance
# ============================================================================
class TestAllDayRecurrenceExdate:
    """All-day events send DATE values, not DATETIME. Ensure EXDATE still works."""

    ALL_DAY_BASE = _BASE.replace(hour=0, minute=0)
    DELETED_DATE = ALL_DAY_BASE + timedelta(weeks=1)

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:allday-exdate-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;VALUE=DATE:{_date(ALL_DAY_BASE)}
DTEND;VALUE=DATE:{_date(ALL_DAY_BASE + timedelta(days=1))}
RRULE:FREQ=WEEKLY;COUNT=4;BYDAY=MO
EXDATE;VALUE=DATE:{_date(DELETED_DATE)}
SUMMARY:All-Day Milestone
END:VEVENT
END:VCALENDAR
"""

    def test_allday_exdate_removes_occurrence(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 3, f"Expected 3 (4 - 1 EXDATE), got {len(starts)}"


# ============================================================================
# 12. STATUS:CANCELLED with no DTSTART/DTEND (minimal cancelled instance)
#     Some Google Calendar feeds send cancelled VEVENTs with only RECURRENCE-ID.
# ============================================================================
class TestStatusCancelledMinimalComponent:
    """
    Google sometimes sends a minimal VEVENT with only UID, RECURRENCE-ID,
    and STATUS:CANCELLED (no DTSTART/DTEND/SUMMARY). The system should still
    handle this gracefully.
    """

    CANCEL_DT = _BASE + timedelta(weeks=2)

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:cancelled-minimal-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=5;BYDAY=MO
SUMMARY:Planning Poker
LOCATION:NSH 3002
END:VEVENT
BEGIN:VEVENT
UID:cancelled-minimal-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{CANCEL_DT.strftime("%Y%m%dT%H%M%S")}
STATUS:CANCELLED
END:VEVENT
END:VCALENDAR
"""

    def test_minimal_cancelled_component_excluded(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 4, (
            f"Expected 4 (5 - 1 minimal cancelled), got {len(starts)}"
        )


# ============================================================================
# 13. Delete missing UIDs: entire recurring event removed from feed
# ============================================================================
class TestDeleteMissingUids:
    """
    When an entire recurring event is removed from the Google Calendar,
    the UID disappears from the feed. With delete_missing_uids=True,
    the event and all its occurrences should be removed.
    """

    ICS_WITH_EVENT = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:removable-event-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=3;BYDAY=MO
SUMMARY:Doomed Meeting
LOCATION:TEP 1403
END:VEVENT
END:VCALENDAR
"""

    ICS_WITHOUT_EVENT = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
END:VCALENDAR
"""

    def test_event_removed_when_uid_missing_from_feed(self, scaffold):
        result1 = _import(scaffold, self.ICS_WITH_EVENT, delete_missing=True)
        assert result1["success"]
        db = scaffold["db"]
        source_id = scaffold["source"].id

        events_before = db.query(Event).filter_by(calendar_source_id=source_id).all()
        assert len(events_before) == 1

        # Second import: event gone from feed
        result2 = _import(scaffold, self.ICS_WITHOUT_EVENT, delete_missing=True)
        assert result2["success"]

        events_after = db.query(Event).filter_by(calendar_source_id=source_id).all()
        assert len(events_after) == 0, "Event should be deleted when UID is missing"


# ============================================================================
# 14. EXDATE in UTC format (Google sometimes sends UTC EXDATEs)
# ============================================================================
class TestExdateUtcFormat:
    """Some feeds send EXDATE in UTC (Z suffix) rather than TZID format."""

    DELETED_DT = _BASE + timedelta(weeks=1)
    DELETED_UTC = DELETED_DT.astimezone(timezone.utc)

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:exdate-utc-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=4;BYDAY=MO
EXDATE:{_ts(DELETED_DT)}
SUMMARY:UTC Exdate Event
LOCATION:Hamburg Hall
END:VEVENT
END:VCALENDAR
"""

    def test_utc_exdate_removes_occurrence(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 3, f"Expected 3 (4 - 1 UTC EXDATE), got {len(starts)}"


# ============================================================================
# 15. Combined: EXDATE + modified RECURRENCE-ID + STATUS:CANCELLED
#     The most complex real-world scenario.
# ============================================================================
class TestComplexRealWorldScenario:
    """
    A weekly event with 6 occurrences where:
    - Week 2 is deleted via EXDATE
    - Week 4 is modified (time changed, still shows up)
    - Week 5 is cancelled via STATUS:CANCELLED
    Expected: 4 occurrences (weeks 1, 3, 4-modified, 6)
    """

    EXDATE_DT = _BASE + timedelta(weeks=1)      # week 2
    MODIFIED_DT = _BASE + timedelta(weeks=3)     # week 4
    MOD_NEW_START = MODIFIED_DT.replace(hour=15)
    MOD_NEW_END = MOD_NEW_START + timedelta(hours=1)
    CANCEL_DT = _BASE + timedelta(weeks=4)       # week 5

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:complex-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=6;BYDAY=MO
EXDATE;TZID=America/New_York:{EXDATE_DT.strftime("%Y%m%dT%H%M%S")}
SUMMARY:Architecture Review
LOCATION:Smith Hall 100
END:VEVENT
BEGIN:VEVENT
UID:complex-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{MODIFIED_DT.strftime("%Y%m%dT%H%M%S")}
DTSTART;TZID=America/New_York:{MOD_NEW_START.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{MOD_NEW_END.strftime("%Y%m%dT%H%M%S")}
SUMMARY:Architecture Review (Moved)
LOCATION:Gates Hillman 6115
END:VEVENT
BEGIN:VEVENT
UID:complex-001@google.com
DTSTAMP:{DTSTAMP}
RECURRENCE-ID;TZID=America/New_York:{CANCEL_DT.strftime("%Y%m%dT%H%M%S")}
DTSTART;TZID=America/New_York:{CANCEL_DT.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(CANCEL_DT + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
STATUS:CANCELLED
SUMMARY:Architecture Review
END:VEVENT
END:VCALENDAR
"""

    def test_complex_scenario_correct_occurrence_count(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        assert len(starts) == 4, (
            f"Expected 4 occurrences (6 - 1 EXDATE - 1 CANCELLED), got {len(starts)}"
        )

    def test_exdate_excluded(self, scaffold):
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        starts = _occurrence_starts(db, event_id)
        exdate_utc = self.EXDATE_DT.astimezone(timezone.utc)
        assert exdate_utc not in starts

    def test_modified_instance_present(self, scaffold):
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        mod_start_utc = self.MOD_NEW_START.astimezone(timezone.utc)
        matching = (
            db.query(EventOccurrence)
            .filter(
                EventOccurrence.event_id == event_id,
                EventOccurrence.start_datetime == mod_start_utc,
            )
            .all()
        )
        assert len(matching) == 1, "Modified instance should be present"

    def test_modified_instance_has_updated_title(self, scaffold):
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        overridden = (
            db.query(EventOccurrence)
            .filter(
                EventOccurrence.event_id == event_id,
                EventOccurrence.title == "Architecture Review (Moved)",
            )
            .all()
        )
        assert len(overridden) == 1

    def test_data_model_integrity(self, scaffold):
        """Verify DB state: 1 event, 1 rule, 2 exdates, 1 override."""
        result = _import(scaffold, self.ICS)
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        rule = db.query(RecurrenceRule).filter_by(event_id=event_id).first()
        assert rule is not None

        exdates = db.query(RecurrenceExdate).filter_by(rrule_id=rule.id).all()
        assert len(exdates) == 2

        overrides = db.query(EventOverride).filter_by(rrule_id=rule.id).all()
        assert len(overrides) == 1, (
            "Should have 1 override (modified instance) and 1 cancelled exdate"
        )


# ============================================================================
# 16. Occurrence generation directly (unit test for populate_event_occurrences)
# ============================================================================
class TestPopulateOccurrencesWithExdates:
    """
    Unit test: create event + rule + exdates directly in DB, then call
    populate_event_occurrences and verify the exclusion.
    """

    def test_populate_skips_exdates(
        self, db, event_factory, recurrence_rule_factory
    ):
        event_tz = ZoneInfo("America/New_York")
        start = datetime(2026, 3, 2, 10, 0, tzinfo=event_tz)
        end = start + timedelta(hours=1)

        event = event_factory(
            start_datetime=start.astimezone(timezone.utc),
            end_datetime=end.astimezone(timezone.utc),
            event_timezone="America/New_York",
        )

        rule = recurrence_rule_factory(
            event_id=event.id,
            frequency="WEEKLY",
            interval=1,
            start_datetime=start.astimezone(timezone.utc),
            count=5,
        )

        # Add EXDATE for week 3
        exdate_dt = (start + timedelta(weeks=2)).astimezone(timezone.utc)
        db.add(RecurrenceExdate(rrule_id=rule.id, exdate=exdate_dt))
        db.flush()

        populate_event_occurrences(db, event, rule)

        occs = db.query(EventOccurrence).filter_by(event_id=event.id).all()
        assert len(occs) == 4, f"Expected 4 (5 - 1 exdate), got {len(occs)}"

        occ_starts = {o.start_datetime for o in occs}
        assert exdate_dt not in occ_starts


# ============================================================================
# 17. Occurrence regeneration idempotency
# ============================================================================
class TestOccurrenceRegenerationIdempotent:
    """
    Re-running populate_event_occurrences should produce the same number
    of occurrences (not duplicate them).
    """

    ICS = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Google Inc//Google Calendar 70.9054//EN
X-WR-TIMEZONE:America/New_York
BEGIN:VEVENT
UID:idempotent-001@google.com
DTSTAMP:{DTSTAMP}
DTSTART;TZID=America/New_York:{_BASE.strftime("%Y%m%dT%H%M%S")}
DTEND;TZID=America/New_York:{(_BASE + timedelta(hours=1)).strftime("%Y%m%dT%H%M%S")}
RRULE:FREQ=WEEKLY;COUNT=4;BYDAY=MO
EXDATE;TZID=America/New_York:{(_BASE + timedelta(weeks=1)).strftime("%Y%m%dT%H%M%S")}
SUMMARY:Idempotent Test
END:VEVENT
END:VCALENDAR
"""

    def test_double_populate_same_count(self, scaffold):
        result = _import(scaffold, self.ICS)
        assert result["success"]
        db = scaffold["db"]
        event_id = result["event_ids"][0]

        count1 = db.query(EventOccurrence).filter_by(event_id=event_id).count()

        # Re-populate
        event = db.query(Event).get(event_id)
        rule = db.query(RecurrenceRule).filter_by(event_id=event_id).first()
        populate_event_occurrences(db, event, rule)

        count2 = db.query(EventOccurrence).filter_by(event_id=event_id).count()
        assert count1 == count2, (
            f"Occurrence count changed from {count1} to {count2} on re-populate"
        )
