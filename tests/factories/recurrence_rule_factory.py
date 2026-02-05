import pytest
from datetime import datetime, timezone
from app.models.models import RecurrenceRule
from app.models.enums import FrequencyType


@pytest.fixture
def recurrence_rule_factory(db):
    def create_rule(**kwargs):
        rule = RecurrenceRule(
            event_id=kwargs.pop("event_id"),
            frequency=kwargs.pop("frequency", FrequencyType.WEEKLY),
            interval=kwargs.pop("interval", 1),
            start_datetime=kwargs.pop(
                "start_datetime",
                datetime.now(timezone.utc),
            ),
            count=kwargs.pop("count", None),
            until=kwargs.pop("until", None),
        )
        db.add(rule)
        db.flush()
        return rule

    return create_rule
