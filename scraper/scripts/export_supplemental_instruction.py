# scraper/scripts/export_supplemental_instruction.py

import logging
import traceback

from app.env import load_env, get_api_base_url
from app.models.category import create_category
from app.models.models import Category
from app.models.organization import create_organization, get_organization_by_name
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
    - Note that nothing rolls back automatically.
    - The system is designed to heal itself on rerun, and does not rely on rollback
    """
    db = get_supabase()

    org = setup_sasc_org(db)

    scraper = SupplementalInstructionScraper(db)
    resources = scraper.scrape_data_only()

    for resource in resources:
        category = setup_sasc_category(db, org, resource.course_num)

        si_event = 






def setup_sasc_org(db):
    """Create the SASC organization if it doesn't already exist."""

    # Check if organization already exists
    existing_org = get_organization_by_name(db, "SASC")
    if existing_org:
        logger.info(f"✅ Organization 'SASC' already exists with ID: {existing_org.id}")
        return existing_org

    org = create_organization(
        db,
        name="SASC",
        description="Student Academic Success Center",
        type="DEPARTMENT",
    )
    db.commit()
    logger.info(f"✅ Created organization 'SASC' with ID: {org.id}")
    return org


def setup_sasc_category(db, org, course_num: str) -> Category:
    """Categories in the SASC organization are named after the course number and store SI and Peer Tutoring events for that course."""

    # Check if category already exists
    existing_category = (
        db.query(Category)
        .filter(Category.org_id == org.id, Category.name == course_num)
        .first()
    )
    if existing_category:
        logger.info(
            f"✅ Category '{course_num}' already exists for organization 'SASC' with ID: {existing_category.id}"
        )
        return existing_category

    category = create_category(db, org_id=org.id, name=course_num)
    db.commit()
    logger.info(
        f"✅ Created category '{course_num}' for organization 'SASC' with ID: {category.id}"
    )
    return category


if __name__ == "__main__":
    export_supplemental_instruction()
