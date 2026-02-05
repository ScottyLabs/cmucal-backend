# app/models/user.py
from app.models.models import User  


def user_to_dict(user):
    return {
        "id": user.id,
        "clerk_id": user.clerk_id,
        "email": user.email,
        "fname": user.fname,
        "lname": user.lname,
        "calendar_id": user.calendar_id,
        "created_at": user.created_at.isoformat() if user.created_at else None,
    }

def create_user(db, clerk_id, **kwargs):
    user = User(clerk_id=clerk_id, **kwargs)
    db.add(user)
    return user

def create_user_without_clerk(db, email, fname=None, lname=None, **kwargs):
    """Create a user without clerk_id (for bulk operations)"""
    user = User(email=email, fname=fname, lname=lname, clerk_id=None, **kwargs)
    db.add(user)
    return user

def get_user_by_email(db, email: str):
    return db.query(User).filter(User.email == email).order_by(User.created_at.asc()).first()

def get_user_by_clerk_id(db, clerk_id):
    return db.query(User).filter(User.clerk_id == clerk_id).first()

def get_user_by_id(db, user_id: int):
    return db.query(User).filter(User.id == user_id).first()
  
def update_user_calendar_id(db, clerk_id, calendar_id):
    user = db.query(User).filter(User.clerk_id == clerk_id).first()
    if not user:
        raise ValueError(f"No user found with clerk_id {clerk_id}")

    user.calendar_id = calendar_id
    return user

