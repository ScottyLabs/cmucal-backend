import pytest
from app.models.models import Organization

# tests/factories/org_factory.py
import pytest
import uuid
from app.models.models import Organization

@pytest.fixture
def org_factory(db):
    def create_org(**kwargs):
        org = Organization(
            name=kwargs.pop("name", f"Test Org {uuid.uuid4()}"),
            tags=kwargs.pop("tags", None),
            description=kwargs.pop("description", None),
            type=kwargs.pop("type", None),
        )
        db.add(org)
        db.flush()
        return org
    return create_org

