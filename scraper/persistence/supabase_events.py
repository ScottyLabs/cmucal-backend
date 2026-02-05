from scraper.persistence.supabase_writer import chunked
from scraper.helpers.event import clean_row_for_insert, event_identity

def insert_events(db, events):
    """
    Returns:
        {event_identity: event_id}
    """

    identity_by_row = []
    rows = []

    for e in events:
        identity_by_row.append(e["_identity"])
        rows.append(clean_row_for_insert({k: v for k, v in e.items() if k != "_identity"}))

    event_id_by_identity = {}

    for batch, id_batch in zip(chunked(rows, 200), chunked(identity_by_row, 200)):
        res = (
            db.table("events")
            .upsert(
                batch,
                on_conflict="org_id,title,semester,start_datetime,end_datetime,location",
                returning="representation",
            )
            .execute()
        )

        for row, identity in zip(res.data, id_batch):
            event_id_by_identity[identity] = row["id"]

    # print(f"Inserted/updated {len(event_id_by_identity)} events")
    return event_id_by_identity
