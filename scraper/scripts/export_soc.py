# scraper/scripts/export_soc.py

import requests
from app.env import load_env, get_api_base_url
ENV = load_env()
API_BASE_URL = get_api_base_url()

from scraper.monitors.academic import ScheduleOfClassesScraper
from scraper.helpers.semester import infer_soc_semester_labels
from scraper.persistence.supabase_categories import ensure_lecture_category
from scraper.persistence.supabase_agent_run import insert_agent_run
from scraper.transforms.soc_org_course import build_orgs_and_courses
from scraper.persistence.supabase_writer import get_supabase
from scraper.persistence.supabase_org_course import upsert_orgs, upsert_courses

from scraper.transforms.soc_events import build_events_and_rrules
from scraper.persistence.supabase_events import insert_events
from scraper.persistence.supabase_recurrence import replace_recurrence_rules

import argparse
import logging
import traceback
from typing import Optional, Sequence

logger = logging.getLogger(__name__)


def _apply_season_flags(
    labels: Sequence[str],
    *,
    spring: bool,
    fall: bool,
) -> list[str]:
    """Narrow inferred labels: only --spring, only --fall, or both (neither or both flags)."""
    if spring == fall:
        return list(labels)
    if spring:
        return [L for L in labels if L.startswith("Spring_")]
    return [L for L in labels if L.startswith("Fall_")]


def export_soc_safe():
    try:
        logger.info("🚀 SOC export started")
        export_soc()
        logger.info("✅ SOC export finished successfully")
    except Exception:
        logger.error("❌ export_soc failed")
        logger.error(traceback.format_exc())

def export_soc(semester_labels: Optional[Sequence[str]] = None):
    """ Scrape the Schedule of Classes and export to Supabase 
        - Note that nothing rolls back automatically.
        - The system is designed to heal itself on rerun, and does not rely on rollback

        semester_labels: if provided, only these labels (e.g. Spring_26) are scraped;
        if None, uses infer_soc_semester_labels() for cron-friendly defaults.
    """
    db = get_supabase()
    labels = (
        list(semester_labels)
        if semester_labels
        else infer_soc_semester_labels()
    )
    logger.info("SOC semesters for this run: %s", ", ".join(labels))
    resources = []
    for label in labels:
        scraper = ScheduleOfClassesScraper(db, semester_label=label)
        resources.extend(scraper.scrape_data_only())

    # agent run
    agent_run_id = insert_agent_run(db, agent_version="soc_v1")
    logger.info(f"Created agent run with ID {agent_run_id}")

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
    events, rrules = build_events_and_rrules(resources, org_id_by_key, category_id_by_org, agent_run_id)
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

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export CMU Schedule of Classes to Supabase.",
    )
    parser.add_argument(
        "semester",
        nargs="*",
        metavar="LABEL",
        help=(
            "Semester label(s) such as Spring_26 or Fall_25. "
            "If omitted, uses automatic current and next terms (see --spring / --fall)."
        ),
    )
    parser.add_argument(
        "--spring",
        action="store_true",
        help=(
            "With no LABEL arguments: only scrape Spring_* from the automatic pair. "
            "Combine with --fall to scrape the full automatic pair."
        ),
    )
    parser.add_argument(
        "--fall",
        action="store_true",
        help=(
            "With no LABEL arguments: only scrape Fall_* from the automatic pair. "
            "Combine with --spring to scrape the full automatic pair."
        ),
    )
    args = parser.parse_args()

    if args.semester:
        if args.spring or args.fall:
            parser.error(
                "Do not use --spring/--fall together with explicit LABEL arguments."
            )
        export_soc(semester_labels=args.semester)
        return

    labels = infer_soc_semester_labels()
    labels = _apply_season_flags(labels, spring=args.spring, fall=args.fall)
    if not labels:
        parser.error("No semesters left after --spring/--fall filter.")
    export_soc(semester_labels=labels)


if __name__ == "__main__":
    main()