from scraper.persistence.supabase_writer import chunked
from scraper.helpers.event import clean_row_for_insert, event_identity

def replace_recurrence_rules(db, rrules, event_id_by_identity):
    rows = []
    event_ids = set()

    for r in rrules:
        identity = r["_identity"]

        if identity not in event_id_by_identity:
            raise RuntimeError(f"ORPHAN RRULE: {identity}")

        (
            _org_id,
            _title,
            _semester,
            start_dt,
            _end_dt,
            _location,
        ) = identity

        event_id = event_id_by_identity[identity]
        event_ids.add(event_id)

        row = {
            **r,
            "event_id": event_id,
        }

        row.pop("_identity")

        rows.append(clean_row_for_insert(row))

    for batch in chunked(list(event_ids), 200):
        db.table("recurrence_rules").delete().in_("event_id", batch).execute()

    db.table("recurrence_rules").insert(rows).execute()
