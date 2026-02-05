# scraper/scripts/export_soc.py

import requests
from app.env import load_env, get_api_base_url
ENV = load_env()
API_BASE_URL = get_api_base_url()

from scraper.monitors.academic import ScheduleOfClassesScraper
from scraper.persistence.supabase_categories import ensure_lecture_category
from scraper.transforms.soc_org_course import build_orgs_and_courses
from scraper.persistence.supabase_writer import get_supabase
from scraper.persistence.supabase_org_course import upsert_orgs, upsert_courses

from scraper.transforms.soc_events import build_events_and_rrules
from scraper.persistence.supabase_events import insert_events
from scraper.persistence.supabase_recurrence import replace_recurrence_rules

import logging
import traceback

logger = logging.getLogger(__name__)

def export_soc_safe():
    try:
        logger.info("🚀 SOC export started")
        export_soc()
        logger.info("✅ SOC export finished successfully")
    except Exception:
        logger.error("❌ export_soc failed")
        logger.error(traceback.format_exc())

def export_soc():
    """ Scrape the Schedule of Classes and export to Supabase 
        - Note that nothing rolls back automatically.
        - The system is designed to heal itself on rerun, and does not rely on rollback
    """
    db = get_supabase()
    scraper = ScheduleOfClassesScraper(db, semester_label="Spring_26")
    resources = scraper.scrape_data_only()

    # orgs + courses
    orgs, courses = build_orgs_and_courses(resources)
    org_id_by_key = upsert_orgs(db, orgs)
    # for (course_num, semester), org_id in org_id_by_key.items():
    #     print(course_num, semester, org_id)
    upsert_courses(db, courses, org_id_by_key)
    print(f"✅ {len(orgs)} orgs and {len(courses)} courses")

    # categories
    category_id_by_org = ensure_lecture_category(db, org_id_by_key)
    print(f"✅ {len(category_id_by_org)} categories")

    # events + recurrence rules
    events, rrules = build_events_and_rrules(resources, org_id_by_key, category_id_by_org)
    event_id_by_identity = insert_events(db, events)
    replace_recurrence_rules(db, rrules, event_id_by_identity)
    print(f"✅ {len(events)} events and {len(rrules)} recurrence rules")

    print(f"...Regenerating occurrences via {API_BASE_URL}")
    affected_event_ids = list(event_id_by_identity.values())
    # Trigger ORM-based regeneration
    if affected_event_ids:
        try:
            requests.post(
                f"{API_BASE_URL}/events/regenerate_occurrences_by_events",
                json={"event_ids": affected_event_ids},
                timeout=5,
            )
        except requests.exceptions.Timeout:
            pass  # regeneration continues server-side
        # print(f"If you see errors here, make sure to start the Flask server before running this script. Timeout errors can be ignored.")
        
    print(f"✅ Called regeneration for {len(affected_event_ids)} events. See logs for details.")

if __name__ == "__main__":
    export_soc()