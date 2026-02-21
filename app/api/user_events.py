from flask import Blueprint, jsonify, request, g
from app.models.user import get_user_by_clerk_id
from app.models.models import Event, UserSavedEvent, EventOccurrence
from datetime import datetime, timezone
import traceback 

user_events_bp = Blueprint("user_events", __name__)

@user_events_bp.route("/user_saved_events", methods=["GET"])
def get_all_saved_events():
    db = g.db
    try:
        clerk_id = request.args.get("user_id")
        if not clerk_id:
            return jsonify({"error": "Missing user_id"}), 400
        user = get_user_by_clerk_id(db, clerk_id)

        # only columns required for calendar view
        events = db.query(Event.id, Event.title, Event.start_datetime, Event.end_datetime)\
            .join(UserSavedEvent).filter(
                UserSavedEvent.user_id == user.id
            ).all()

        return jsonify([e[0] for e in events]) 

    except Exception as e:
        print("Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@user_events_bp.route("/user_saved_event_occurrences", methods=["GET"])
def get_all_saved_events_occurrences():
    db = g.db
    try:
        # get user
        clerk_id = request.args.get("user_id")
        if not clerk_id:
            return jsonify({"error": "Missing user_id"}), 400
        user = get_user_by_clerk_id(db, clerk_id)

        event_occurrences = (db.query(EventOccurrence.id, EventOccurrence.title, 
        EventOccurrence.start_datetime, EventOccurrence.end_datetime, Event.id)
            .join(Event, EventOccurrence.event_id == Event.id)
            .join(UserSavedEvent, UserSavedEvent.event_id == Event.id)
            .filter(
                UserSavedEvent.user_id == user.id
            ).all())

        return [
            {
                "id": e[0],
                "title": e[1],
                "start": e[2].isoformat(),
                "end": e[3].isoformat(),
                "event_id": e[4]
            }
            for e in event_occurrences
        ]

    except Exception as e:
        print("Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@user_events_bp.route("/user_saved_events", methods=["POST"])
def user_save_event():
    db = g.db
    try:
        data = request.get_json()
        # get user
        clerk_id = data.get("user_id")
        if not clerk_id:
            return jsonify({"error": "Missing user_id"}), 400
        user = get_user_by_clerk_id(db, clerk_id)

        new_entry = UserSavedEvent(
            user_id = user.id,
            event_id = data["event_id"],
            google_event_id = data["google_event_id"],
            saved_at = datetime.now(timezone.utc),
        )
        db.add(new_entry)
        db.commit()
        return jsonify({"message": "Event added to user's saved events."}), 201
        
    except Exception as e:
        print("Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@user_events_bp.route("/user_saved_events/<event_id>", methods=["DELETE"])
def user_unsave_event(event_id):
    db = g.db
    try:
        data = request.get_json()
        # get user
        clerk_id = data.get("user_id")
        if not clerk_id:
            return jsonify({"error": "Missing user_id"}), 400
        user = get_user_by_clerk_id(db, clerk_id)

        user_id = user.id
        entry = db.query(UserSavedEvent).filter_by(user_id=user_id, event_id=event_id).first()

        if not entry:
            return jsonify({"error": "Saved event not found"}), 404
        db.delete(entry)
        db.commit()
        return jsonify({"message": "Event removed from user's saved events."}), 200 
            
    except Exception as e:
        print("Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500
