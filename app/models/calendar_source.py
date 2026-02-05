from app.models.models import CalendarSource
from typing import Optional
from sqlalchemy.orm import Session
from datetime import datetime, timezone

def create_calendar_source(
    db_session,
    *,
    url: str,
    org_id: int,
    category_id: int,
    active: bool = True,
    notes: Optional[str] = None,
    default_event_type: Optional[str] = None,
    created_by_user_id: Optional[int] = None,
    **kwargs,
):
    calendar_source = CalendarSource(
        url=url,
        org_id=org_id,
        category_id=category_id,
        active=active,
        notes=notes,
        default_event_type=default_event_type,
        created_by_user_id=created_by_user_id,
        **kwargs,
    )

    db_session.add(calendar_source)
    db_session.flush()
    return calendar_source


def deactivate_calendar_source(
    db: Session,
    calendar_source_id: int,
) -> CalendarSource:
    """
    Mark a CalendarSource as inactive.

    - Acquires a row-level lock
    - Updates active flag and updated_at
    - Does NOT commit (caller controls transaction)

    Returns:
        The updated CalendarSource
    """

    calendar_source = (
        db.query(CalendarSource)
        .filter(CalendarSource.id == calendar_source_id)
        .with_for_update()
        .one_or_none()
    )

    if not calendar_source:
        raise ValueError("CalendarSource not found")

    if calendar_source.active:
        calendar_source.active = False
        calendar_source.updated_at = datetime.now(timezone.utc)

    db.flush()
    return calendar_source
