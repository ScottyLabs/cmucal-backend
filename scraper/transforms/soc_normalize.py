def normalize_soc_rows(rows):
    normalized = []

    for r in rows:
        normalized.append({
            "course_num": r.course_num,
            "course_name": r.course_name.strip(),
            "lecture_section": r.lecture_section,
            "lecture_days": r.lecture_days,
            "start_time": r.lecture_time_start,
            "end_time": r.lecture_time_end,
            "location": r.location,
            "semester": r.semester,
        })

    return normalized
