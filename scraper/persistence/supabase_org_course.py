from scraper.persistence.supabase_writer import chunked


def upsert_orgs(db, orgs: dict) -> dict:
    """
    Upserts organizations and returns a mapping:
        {(course_num, semester): org_id}
    """
    data = []

    for org in orgs.values():
        clean = dict(org)
        clean.pop("id", None)  # Remove id if present
        clean.pop("created_at", None)
        data.append(clean)

    # Upsert: insert or update on conflict
    for batch in chunked(data, 200):
        db.table("organizations").upsert(
            batch,
            on_conflict="name"
        ).execute()

    # Fetch IDs back
    name_to_id = {}
    names = [o["name"] for o in data]

    for batch in chunked(names, 200):  # 200 is safe
        res = db.table("organizations").select("id,name").in_("name", batch).execute()
        for row in res.data:
            name_to_id[row["name"]] = row["id"]

    return {key: name_to_id[org["name"]] for key, org in orgs.items()}


def upsert_courses(db, courses: dict, org_id_by_key: dict):
    """
    Upserts courses and appends semester if it already exists.
    """

    course_numbers = [c["course_number"] for c in courses.values()]

    # Fetch existing courses
    existing_by_number = {}
    for batch in chunked(course_numbers, 200):
        res = (
            db.table("courses")
            .select("id, course_number, semesters")
            .in_("course_number", batch)
            .execute()
            .data
        )
        for row in res:
            existing_by_number[row["course_number"]] = row

    # Merge semesters
    rows_to_upsert = []

    for (course_num, semester), course in courses.items():
        org_id = org_id_by_key[(course_num, semester)]

        if course_num in existing_by_number:
            existing_semesters = existing_by_number[course_num]["semesters"] or []
            merged_semesters = sorted(set(existing_semesters + course["semesters"]))
        else:
            merged_semesters = course["semesters"]

        rows_to_upsert.append(
            {
                "course_number": course_num,
                "course_name": course["course_name"],
                "semesters": merged_semesters,
                "org_id": org_id,
            }
        )

    # Upsert merged result
    db.table("courses").upsert(rows_to_upsert, on_conflict="course_number").execute()
