from scraper.persistence.supabase_writer import chunked

def ensure_lecture_category(db, org_id_by_key: dict) -> dict:
    CATEGORY_NAME = "Lecture and Recitations"
    org_ids = list(set(org_id_by_key.values()))

    # ---- fetch existing categories (chunked) ----
    existing = []
    for batch in chunked(org_ids, 200):
        res = (
            db.table("categories")
            .select("id, org_id, name")
            .in_("org_id", batch)
            .execute()
            .data
        )
        existing.extend(res)

    category_by_org = {
        row["org_id"]: row["id"] for row in existing if row["name"] == CATEGORY_NAME
    }

    # ---- insert missing categories ----
    rows_to_insert = []
    for org_id in org_ids:
        if org_id not in category_by_org:
            rows_to_insert.append(
                {
                    "org_id": org_id,
                    "name": CATEGORY_NAME,
                }
            )

    if rows_to_insert:
        # defensive cleanup
        cleaned = []
        for row in rows_to_insert:
            clean = dict(row)
            clean.pop("id", None)
            clean.pop("created_at", None)
            cleaned.append(clean)

        db.table("categories").insert(cleaned).execute()

    # ---- refetch IDs ----
    all_categories = []
    for batch in chunked(org_ids, 200):
        res = (
            db.table("categories")
            .select("id, org_id, name")
            .in_("org_id", batch)
            .execute()
            .data
        )
        all_categories.extend(res)

    return {
        row["org_id"]: row["id"]
        for row in all_categories
        if row["name"] == CATEGORY_NAME
    }
