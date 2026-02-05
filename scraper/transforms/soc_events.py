from scraper.helpers.recurrence import parse_soc_time, build_rrule_from_parts
from scraper.helpers.event import event_identity, group_soc_rows
from scraper.helpers.timezone import timezone_from_location

def build_events_and_rrules(soc_rows, org_id_by_key, category_id_by_org):
    from collections import defaultdict
    import datetime

    # Group SOC rows into events
    grouped = group_soc_rows(soc_rows)

    events = []
    rrules = []

    for (
        course_num,
        lecture_section,
        semester,
        time_start,
        time_end,
        location,
    ), rows in grouped.items():

        soc0 = rows[0]

        org_id = org_id_by_key[(course_num, semester)]
        category_id = category_id_by_org[org_id]
        tz = timezone_from_location(location)

        start_dt = datetime.datetime.combine(
            soc0.sem_start,
            parse_soc_time(time_start),
            tzinfo=tz,
        )
        end_dt = datetime.datetime.combine(
            soc0.sem_start,
            parse_soc_time(time_end),
            tzinfo=tz,
        )

        title = f"{course_num} {lecture_section}"
        description = f"{soc0.course_name} :: {course_num} {lecture_section}"

        identity = event_identity(
            org_id,
            title,
            semester,
            start_dt,
            end_dt,
            location,
        )

        event = {
            "org_id": org_id,
            "title": title,
            "semester": semester,
            "start_datetime": start_dt,
            "end_datetime": end_dt,
            "location": location,
            "is_all_day": False,
            "event_timezone": str(tz),
            "category_id": category_id,
            "description": description,
            "event_type": "ACADEMIC",
            "source_url": "https://enr-apps.as.cmu.edu/open/SOC/SOCServlet/completeSchedule",
            "_identity": identity,  # runtime-only
        }

        events.append(event)

        all_days = set()
        for soc in rows:
            all_days.update(soc.lecture_days)

        rrule = build_rrule_from_parts(
            lecture_days="".join(sorted(all_days)),
            sem_start=soc0.sem_start,
            sem_end=soc0.sem_end,
            start_time=parse_soc_time(time_start),
            tz=tz,
        )

        if rrule:
            rrule["_identity"] = identity
            rrules.append(rrule)

    assert len(events) == len(rrules)
    return events, rrules