# scraper/scripts/export_supplemental_instruction.py

from datetime import timedelta
from dateutil.parser import isoparse
from zoneinfo import ZoneInfo

from app.env import load_env, get_api_base_url
from app.models.enums import FrequencyType
from scraper.helpers.event import event_identity
from scraper.monitors.academic import SupplementalInstructionScraper
from scraper.persistence.supabase_writer import get_supabase
from scraper.persistence.supabase_events import insert_events
from scraper.persistence.supabase_recurrence import replace_recurrence_rules

ENV = load_env()
API_BASE_URL = get_api_base_url()

SI_SOURCE_URL = "https://www.cmu.edu/student-success/programs/supp-inst.html"
SI_SEMESTER = "SI"
SI_TIMEZONE = ZoneInfo("America/New_York")
CLEAR_CATEGORIES = True


def export_supplemental_instruction():
    db = get_supabase()
    org = setup_sasc_org(db)
    scraper = SupplementalInstructionScraper(db)
    resources = scraper.scrape_data_only()

    events = []
    rrules = []

    for resource in resources:
        category = setup_sasc_category(db, org, resource.course_num, clear=CLEAR_CATEGORIES)
        for time_location in resource.time_locations:
            event, rrule = create_si_event(resource, org, category, time_location)
            events.append(event)
            rrules.append(rrule)

    if events:
        event_id_by_identity = insert_events(db, events)
        replace_recurrence_rules(db, rrules, event_id_by_identity)


def create_si_event(resource, org: dict, category: dict, time_location: dict, *, semester: str = SI_SEMESTER):
    location = time_location["location"]
    tz = SI_TIMEZONE

    # Parse start and end datetimes, and ensure they are timezone-aware
    start_dt = isoparse(time_location["start_datetime"])
    end_dt = isoparse(time_location["end_datetime"])
    start_dt = start_dt.astimezone(tz)
    end_dt = end_dt.astimezone(tz)

    
    org_id = org["id"]
    category_id = category["id"]
    by_day = time_location["recurrence_by_day"]
    title = f"SI {resource.course_num} [{by_day}] ({location})"
    description = f"{resource.course_name} - SI with {', '.join(resource.si_leaders)}"
    identity = event_identity(org_id, title, semester, start_dt, end_dt, location)

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
        "source_url": SI_SOURCE_URL,
        "_identity": identity,
    }

    until_dt = start_dt + timedelta(days=180)
    rrule = {
        "frequency": FrequencyType.WEEKLY,
        "interval": time_location.get("recurrence_interval", 1),
        "by_day": [by_day],
        "until": until_dt,
        "start_datetime": start_dt,
        "count": None,
        "by_month": None,
        "by_month_day": None,
        "orig_until": until_dt,
        "_identity": identity,
    }

    return event, rrule


def setup_sasc_org(db):
    # Check if SASC organization already exists
    res = db.table("organizations").select("id, name").eq("name", "SASC").execute()
    if res.data:
        return {"id": res.data[0]["id"], "name": res.data[0]["name"]}
    
    # Create SASC organization if it doesn't exist
    db.table("organizations").insert({
        "name": "SASC",
        "description": "Student Academic Success Center",
        "type": "DEPARTMENT",
    }).execute()
    res = db.table("organizations").select("id, name").eq("name", "SASC").execute()
    row = res.data[0]
    return {"id": row["id"], "name": row["name"]}


def setup_sasc_category(db, org: dict, course_num: str, clear: bool = False) -> dict:
    # Check if SI category already exists
    category_name = f"SI {course_num}"
    org_id = org["id"]

    res = (
        db.table("categories")
        .select("id, org_id, name")
        .eq("org_id", org_id)
        .eq("name", category_name)
        .execute()
    )

    # If category already exists, return it. Otherwise, create it.
    if res.data:
        row = res.data[0]
        category_id = row["id"]
    else:
        db.table("categories").insert({"org_id": org_id, "name": category_name}).execute()
        res = (
            db.table("categories")
            .select("id, org_id, name")
            .eq("org_id", org_id)
            .eq("name", category_name)
            .execute()
        )
        row = res.data[0]
        category_id = row["id"]

    # Clear events from category if clear is True
    if clear and category_id:
        db.table("events").delete().eq("category_id", category_id).execute()

    return {"id": row["id"], "org_id": org_id, "name": row["name"]}


if __name__ == "__main__":
    export_supplemental_instruction()
