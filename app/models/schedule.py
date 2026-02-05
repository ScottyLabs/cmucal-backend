from app.models.models import Schedule

def create_schedule(db, user_id: int, name: str):
    """
    Create a new schedule in the database.
    
    Args:
        db: Database session.
        user_id: ID of the user.
        name: Name of the schedule.
        
    Returns:
        The created Schedule object.
    """
    schedule = Schedule(user_id=user_id, name=name)
    db.add(schedule)
    return schedule

def delete_schedule(db, schedule_id: int):
    """
    Delete a schedule from the database.
    
    Args:
        db: Database session.
        schedule_id: ID of the schedule to delete.
        
    Returns:
        True if deleted, False if not found.
    """
    schedule = db.query(Schedule).filter(Schedule.id == schedule_id).first()
    if schedule:
        db.delete(schedule)
        return True
    return False