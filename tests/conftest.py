# tests/conftest.py
import os
os.environ["APP_ENV"] = "test"
os.environ["ALLOW_TEST_DB"] = "1"

import pytest
from sqlalchemy.orm import Session
import itertools

from app import create_app
from app.services.db import init_db
from app.models.models import User, Organization
from app.models.admin import create_admin


# ---------- Flask App ----------
@pytest.fixture(scope="session")
def app():
    app = create_app()
    app.config["TESTING"] = True
    yield app


# ---------- Flask Test Client ----------
@pytest.fixture
def client(app):
    return app.test_client()


# ---------- DB Engine (session-wide) ----------
@pytest.fixture(scope="session")
def engine(app):
    engine, _ = init_db()
    return engine


# ---------- DB Session (per test, rolled back) ----------
@pytest.fixture
def db(engine):
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()

# ---------- Forbid real LLM calls ----------
@pytest.fixture(autouse=True)
def forbid_real_llm_calls(mocker):
    mocker.patch(
        'course_agent.app.services.llm.get_llm',
        side_effect=AssertionError('LLM accessed without mock')
    )

# ---------- Forbid real Search API calls ----------
def forbid_real_search_calls(mocker):
    mocker.patch(
        'course_agent.app.agent.nodes.search.search',
        side_effect=AssertionError(
            'Search accessed without mock. Patch course_agent.app.agent.nodes.search.search'
        )
    )

# ---------- FACTORY FIXTURES ----------
from tests.factories.user_factory import *
from tests.factories.org_factory import *
from tests.factories.admin_factory import *
from tests.factories.course_factory import *
from tests.factories.course_agent_state_factory import *
from tests.factories.category_factory import *
from tests.factories.event_factory import *
from tests.factories.calendar_source_factory import *
from tests.factories.recurrence_rule_factory import *
