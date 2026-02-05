import pytest
from app.models.models import User

@pytest.fixture
def user_factory(db):
    def create_user(**kwargs):
        user = User(
            email=kwargs.pop("email", "user@test.com"),
            clerk_id=kwargs.pop("clerk_id", "clerk_test"),
            **kwargs,
        )
        db.add(user)
        db.flush()
        return user
    return create_user
