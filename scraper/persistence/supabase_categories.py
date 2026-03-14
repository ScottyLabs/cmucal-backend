from scraper.persistence.supabase_writer import chunked




def ensure_lecture_category(db, org_id_by_key: dict) -> dict:
    """
    Returns:
        {
            org_id: {
                "LECTURE": lecture_category_id,
                "RECITATION": recitation_category_id
            }
        }
    """
    CATEGORY_NAMES = {
        "LECTURE": "Lectures",
        "RECITATION": "Recitations",
    }

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

    category_map = {}

    # Build existing lookup
    for row in existing:
        if row["org_id"] not in category_map:
            category_map[row["org_id"]] = {}

        for key, name in CATEGORY_NAMES.items():
            if row["name"] == name:
                category_map[row["org_id"]][key] = row["id"]
    # Insert missing
    rows_to_insert = []
    for org_id in org_ids:
        category_map.setdefault(org_id, {})
        for key, name in CATEGORY_NAMES.items():
            if key not in category_map[org_id]:
                rows_to_insert.append({
                    "org_id": org_id,
                    "name": name,
                })

    if rows_to_insert:
        # defensive cleanup
        cleaned = []
        for row in rows_to_insert:
            clean = dict(row)
            clean.pop("id", None)
            clean.pop("created_at", None)
            cleaned.append(clean)

        db.table("categories").insert(cleaned).execute()

    # re-fetch to get IDs of newly inserted categories
    refreshed = []
    for batch in chunked(org_ids, 200):
        res = (
            db.table("categories")
            .select("id, org_id, name")
            .in_("org_id", batch)
            .execute()
            .data
        )
        refreshed.extend(res)

    result = {}
    for row in refreshed:
        org_id = row["org_id"]
        result.setdefault(org_id, {})
        for key, name in CATEGORY_NAMES.items():
            if row["name"] == name:
                result[org_id][key] = row["id"]
    return result