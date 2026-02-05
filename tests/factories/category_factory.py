import pytest
from app.models.models import Category

@pytest.fixture
def category_factory(db):
    def create_category(**kwargs):
        category = Category(
            name=kwargs.pop("name", "Test Category"),
            org_id=kwargs.pop("org_id"),
            **kwargs,
        )
        db.add(category)
        db.flush()
        return category
    return create_category
