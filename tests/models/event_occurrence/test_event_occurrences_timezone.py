from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.models.models import EventOccurrence
from app.models.enums import FrequencyType
from app.models.event_occurrence import populate_event_occurrences


def test_recurring_event_occurrences_respect_local_time_across_dst(
    db,
    event_factory,
    recurrence_rule_factory,
):
    """
    Regression test:
    Weekly recurring events must stay at the same LOCAL time (6 PM)
    across DST changes, even though UTC offsets change.
    """

    NY = ZoneInfo("America/New_York")

    # ── Arrange ───────────────────────────────────────────────

    # Base event: 6:00 PM America/New_York (local time)
    now_local = datetime.now(NY)
    start_local = (
        now_local
        .replace(hour=18, minute=0, second=0, microsecond=0)
        + timedelta(days=1)
    )
    end_local = start_local + timedelta(hours=4)
    event_timezone = "America/New_York"

    event = event_factory(
        start_datetime=start_local.astimezone(timezone.utc),
        end_datetime=end_local.astimezone(timezone.utc),
        event_timezone=event_timezone,
        location="DST Test Location",
        semester="Fall_25",
        title="DST Test Event",
    )

    rule = recurrence_rule_factory(
        event_id=event.id,
        frequency=FrequencyType.WEEKLY,
        interval=1,
        start_datetime=event.start_datetime,  # stored in UTC (correct)
        count=20,  # enough to cross DST boundary
    )

    # ── Act ───────────────────────────────────────────────────

    populate_event_occurrences(db, event, rule)
    db.flush()

    occurrences = (
        db.query(EventOccurrence)
        .filter_by(event_id=event.id)
        .order_by(EventOccurrence.start_datetime)
        .all()
    )

    assert len(occurrences) >= 10

    # ── Assert ────────────────────────────────────────────────

    # Group occurrences by local UTC offset
    by_offset = {}
    for o in occurrences:
        local = o.start_datetime.astimezone(NY)
        by_offset.setdefault(local.utcoffset(), []).append(local)

    # We must see at least two different offsets (DST + standard time)
    assert len(by_offset) >= 2, "Expected occurrences across DST boundary"

    # Pick one occurrence from each offset
    (offset1, occs1), (offset2, occs2) = list(by_offset.items())[:2]

    occ1 = occs1[0]
    occ2 = occs2[0]

    # 🔑 Core invariant: local time must stay 6 PM
    assert occ1.hour == 18
    assert occ2.hour == 18

    # Sanity: offsets really differ
    assert offset1 != offset2
