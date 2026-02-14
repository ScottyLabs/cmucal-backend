from zoneinfo import ZoneInfo
from flask import Blueprint, jsonify, request, g
from app.models.tag import get_all_tags
from app.models.event_tag import get_tags_by_event

tags_bp = Blueprint("tags", __name__)

@tags_bp.route("/", methods=["GET"])
def get_tags():
    db = g.db
    try: 
        tags = get_all_tags(db)
        return jsonify([{"name": tag.name, "id": tag.id} for tag in tags]), 200
    except Exception as e:
        print("Exception:", e)
        return jsonify({"error": str(e)}), 500

@tags_bp.route("/<event_id>", methods=["GET"])
def get_event_tags(event_id):
    db = g.db
    try:
        tags = get_tags_by_event(db, event_id)
        tag_names = [{"id": t.id, "name": t.name} for t in tags]
        return tag_names

    except Exception as e:
        import traceback
        print("Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500
