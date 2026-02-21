# app/helpers/event_type.py

from app.models.career import save_career
from app.models.academic import save_academic
from app.models.club import save_club


def handle_event_type(db, event_id: int, event_type: str, data: dict):
    """
    Save the event type-specific record (Career, Academic, or Club)
    based on the event type.
    """
    if event_type == "CAREER":
        save_career(
            db,
            event_id=event_id,
            host=data.get("host"),
            link=data.get("link"),
            registration_required=data.get("registration_required")
        )

    elif event_type == "ACADEMIC":
        course_num = data.get("course_num")
        course_name = data.get("course_name")
        if not course_num or not course_name:
            raise ValueError("Missing required fields for academic event: course_num, course_name")
        save_academic(
            db,
            event_id=event_id,
            course_num=course_num,
            course_name=course_name,
            instructors=data.get("instructors")
        )

    elif event_type == "CLUB":
        save_club(db, event_id=event_id)