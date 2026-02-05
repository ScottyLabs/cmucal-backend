from app.models.models import Course 

def create_course(db, course_number: int, course_name: str, org_id: int, semesters: list = None):
    """
    Create a new course in the database.

    Args:
        db: Database session.
        course_number: Number of the course.
        course_name: Name of the course.
        org_id: ID of the organization to which the course belongs.
        semesters: List of semesters in which the course is offered (optional).
    Returns:
        The created Course object.
    """
    course = Course(course_name=course_name, course_number=course_number, org_id=org_id, semesters=semesters)
    db.add(course)
    return course