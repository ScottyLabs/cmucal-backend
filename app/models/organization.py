from typing import List
from app.models.models import Organization
from app.utils.course_data import get_course_data

def create_organization(db, name: str, description: str = None, type: str = None):
    """
    Create a new organization in the database.

    Args:
        db: Database session.
        name: Name of the organization.
        description: Description of the organization (optional).

    Returns:
        The created Organization object.
    """
    org = Organization(name=name, description=description, type=type)
    db.add(org)
    return org

def get_orgs_by_type(db, org_type: str):
    """
    Fetch all organizations of a specific type.
    """
    return db.query(Organization).filter(Organization.type == org_type).all()

def get_organization_by_id(db, org_id: int):
    """
    Retrieve an organization by its ID.
    Args:
        db: Database session.
        org_id: ID of the organization.
    Returns:
        The Organization object if found, otherwise None.
    """
    return db.query(Organization).filter(Organization.id == org_id).first()

def get_organization_by_name(db, name: str):
    """
    Retrieve an organization by its name.
    Args:
        db: Database session.
        name: Name of the organization.
    Returns:
        The Organization object if found, otherwise None.
    """
    return db.query(Organization).filter(Organization.name == name).first()

def delete_organization(db, org_id: int):
    """
    Delete an organization by its ID.

    Args:
        db: Database session.
        org_id: ID of the organization to delete.

    Returns:
        True if the organization was deleted, False if it was not found.
    """
    org = db.query(Organization).filter(Organization.id == org_id).first()
    if org:
        db.delete(org)
        return True
    return False

