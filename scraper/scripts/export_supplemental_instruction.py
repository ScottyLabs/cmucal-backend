# scraper/scripts/export_supplemental_instruction.py

import logging
import traceback

from app.env import load_env, get_api_base_url
from scraper.monitors.academic import SupplementalInstructionScraper
from scraper.persistence.supabase_writer import get_supabase

ENV = load_env()
API_BASE_URL = get_api_base_url()

logger = logging.getLogger(__name__)


def export_supplemental_instruction_safe():
    # try:
    #     logger.info("🚀 Supplemental Instruction export started")
    #     export_supplemental_instruction()
    #     logger.info("✅ Supplemental Instruction export finished successfully")
    # except Exception:
    #     logger.error("❌ export_supplemental_instruction failed")
    #     logger.error(traceback.format_exc())
    pass


def export_supplemental_instruction():
    """Scrape Supplemental Instruction data and export to Supabase
    # - Note that nothing rolls back automatically.
    # - The system is designed to heal itself on rerun, and does not rely on rollback
    """
    db = get_supabase()

    org = setup_sasc_org(db)

    scraper = SupplementalInstructionScraper(db)
    resources = scraper.scrape_data_only()

    for resource in resources:
        category = setup_sasc_category(db, org, resource.course_num)
        
        
        print(f"Course Number: {resource.course_num}")
        print(f"Course Name: {resource.course_name}")
        print(f"Professors: {resource.professors}")
        print(f"SI Leaders: {resource.si_leaders}")
        print(f"Time Locations: {resource.time_locations}")
        print("--------------------------------")






def setup_sasc_org(db):
    """Create the SASC organization if it doesn't already exist. Uses Supabase client; returns a dict with id, name."""

    res = db.table("organizations").select("id, name").eq("name", "SASC").execute()
    if res.data:
        row = res.data[0]
        logger.info(f"✅ Organization 'SASC' already exists with ID: {row['id']}")
        return {"id": row["id"], "name": row["name"]}

    db.table("organizations").insert({
        "name": "SASC",
        "description": "Student Academic Success Center",
        "type": "DEPARTMENT",
    }).execute()
    res = db.table("organizations").select("id, name").eq("name", "SASC").execute()
    row = res.data[0]
    logger.info(f"✅ Created organization 'SASC' with ID: {row['id']}")
    return {"id": row["id"], "name": row["name"]}


def setup_sasc_category(db, org: dict, course_num: str) -> dict:
    """SI Categories in the SASC organization are named after the course number. Uses Supabase client; returns a dict with id."""

    category_name = f"SI {course_num}"
    org_id = org["id"]

    res = (
        db.table("categories")
        .select("id, org_id, name")
        .eq("org_id", org_id)
        .eq("name", category_name)
        .execute()
    )
    if res.data:
        row = res.data[0]
        logger.info(
            f"✅ Category '{category_name}' already exists for organization 'SASC' with ID: {row['id']}"
        )
        return {"id": row["id"], "org_id": org_id, "name": row["name"]}

    db.table("categories").insert({"org_id": org_id, "name": category_name}).execute()
    res = (
        db.table("categories")
        .select("id, org_id, name")
        .eq("org_id", org_id)
        .eq("name", category_name)
        .execute()
    )
    row = res.data[0]
    logger.info(
        f"✅ Created category '{category_name}' for organization 'SASC' with ID: {row['id']}"
    )
    return {"id": row["id"], "org_id": org_id, "name": row["name"]}


if __name__ == "__main__":
    export_supplemental_instruction()
