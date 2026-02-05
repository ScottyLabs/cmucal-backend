from sqlite3 import IntegrityError
from flask import Blueprint, jsonify, request, g
from app.models.user import get_user_by_clerk_id, get_user_by_email, create_user, user_to_dict
from app.services.google_service import fetch_user_credentials
from app.models.user import update_user_calendar_id
from app.services.google_service import create_cmucal_calendar
from app.models.organization import create_organization
from app.models.admin import create_admin, get_categories_for_admin_user, get_role
from app.models.schedule import create_schedule, delete_schedule
from app.models.schedule_category import create_schedule_category
from app.models.schedule_org import create_schedule_org, remove_schedule_org
from app.models.category import join_org_and_to_dict
from app.models.models import User
from contextlib import contextmanager

users_bp = Blueprint("users", __name__)

@users_bp.route("/get_user_id", methods=["GET"])
def get_user_id():
    db = g.db
    try:
        clerk_id = request.headers.get('Clerk-User-Id')
        if not clerk_id:
            return jsonify({"error": "Missing clerk_id"}), 400
        
        user = get_user_by_clerk_id(db, clerk_id)
        if user is None:
            return jsonify({"error": "User not found"}), 404
        
        return jsonify({"user_id": user.id}), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@users_bp.route("/login", methods=["POST"])
def handle_login():
    db = g.db
    try:
        data = request.get_json()
        clerk_id = data.get("clerk_id")
        email = data.get("email")
        fname = data.get("fname")
        lname = data.get("lname")

        if not clerk_id or not email:
            return jsonify({"error": "Missing clerk_id or email"}), 400
        
        # 1) Find by EMAIL first
        user = get_user_by_email(db, email)
        # print("→ Fetched user by email:", user)

        if user:
            # If the stored clerk_id is missing or differs, update it to the one we just got
            if user.clerk_id != clerk_id:
                # if another row is already using this clerk_id, clear it first to avoid unique conflicts
                other = get_user_by_clerk_id(db, clerk_id)
                if other and other.id != user.id:
                    other.clerk_id = None  # or handle a merge if you keep data on 'other'
                    db.add(other)

                user.clerk_id = clerk_id
                db.add(user)
                # print("→ Updated user with new clerk_id:", user)
                try:
                    db.commit()
                except IntegrityError:
                    return jsonify({"error": "clerk_id already linked to another user"}), 409

                # db.refresh(user)
        else:
            # 2) If not found by email, find by CLERK_ID
            user = get_user_by_clerk_id(db, clerk_id)
            if user is None:
                create_user(
                    db, clerk_id,
                    email=email,
                    fname=fname,
                    lname=lname
                )
                db.commit()
                # re-fetch to get the DB-generated _id and calendar_id
                user = get_user_by_clerk_id(db, clerk_id)
                # print("→ Created user:", user)
                # print("→ Dict:", user_to_dict(user))

        return jsonify({"status": "exists", "user": user_to_dict(user)}), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@users_bp.route("/create_schedule", methods=["POST"])
def create_schedule_record():
    db = g.db
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        name = data.get("name")
        if not user_id or not name:
            return jsonify({"error": "Missing user_id or name"}), 400
        
        schedule = create_schedule(db, user_id=user_id, name=name)
        db.commit()
        return jsonify({"status": "schedule created", "user_id": user_id, "schedule_id": schedule.id}), 201
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@users_bp.route("/delete_schedule", methods=["DELETE"])
def delete_schedule_record():
    db = g.db
    try:
        # Try to get data from JSON body, fallback to query args
        data = request.get_json(silent=True) or {}
        schedule_id = data.get("schedule_id") or request.args.get("schedule_id")
        
        if not schedule_id:
            return jsonify({"error": "Missing schedule_id"}), 400
        
        success = delete_schedule(db, schedule_id=schedule_id)
        db.commit()
        
        if success:
            return jsonify({"status": "schedule deleted", "schedule_id": schedule_id}), 200
        else:
            return jsonify({"error": "Schedule not found"}), 404
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@users_bp.route("/create_schedule_category", methods=["POST"])
def create_schedule_category_record():
    db = g.db
    try:
        data = request.get_json()
        schedule_id = data.get("schedule_id")
        category_id = data.get("category_id")
        if not schedule_id or not category_id:
            return jsonify({"error": "Missing schedule_id or category_id"}), 400
        
        schedule_category = create_schedule_category(db, schedule_id=schedule_id, category_id=category_id)
        db.commit()
        return jsonify({"status": "schedule created", "schedule_id": schedule_id, "category_id": category_id}), 201
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@users_bp.route("/add_org_to_schedule", methods=["POST"])
def add_org_to_schedule():
    db = g.db
    try:
        data = request.get_json()
        schedule_id = data.get("schedule_id")
        org_id = data.get("org_id")
        if not schedule_id or not org_id:
            return jsonify({"error": "Missing schedule_id or org_id"}), 400
        
        schedule_org = create_schedule_org(db, schedule_id=schedule_id, org_id=org_id)
        db.commit()
        return jsonify({"status": "organization added to schedule", "schedule_id": schedule_id, "org_id": org_id}), 201
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@users_bp.route("/remove_org_from_schedule", methods=["POST"])
def remove_org_from_schedule():
    db = g.db
    try:
        data = request.get_json()
        schedule_id = data.get("schedule_id")
        org_id = data.get("org_id")
        if not schedule_id or not org_id:
            return jsonify({"error": "Missing schedule_id or org_id"}), 400
        
        success = remove_schedule_org(db, schedule_id=schedule_id, org_id=org_id)
        db.commit()
        if success:
            return jsonify({"status": "organization removed from schedule", "schedule_id": schedule_id, "org_id": org_id}), 200
        else:
            return jsonify({"error": "Organization not found in schedule"}), 404
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500
        

@users_bp.route("/schedules", methods=["GET"])
def get_user_schedules():
    db = g.db
    try:
        user_id = request.args.get("user_id")
        if not user_id:
            return jsonify({"error": "Missing user_id"}), 400

        # Query user's schedules with schedule_orgs
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return jsonify({"error": "User not found"}), 404

        # Convert schedules to dict format with schedule_orgs
        schedules = [{
            "id": schedule.id,
            "name": schedule.name,
            "schedule_orgs": [{
                "org_id": org.org_id,
                "org_name": org.org.name if org.org else None
            } for org in schedule.schedule_orgs]
        } for schedule in user.schedules]
        return jsonify(schedules), 200

    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@users_bp.route("/get_admin_categories", methods=["GET"])
def get_admin_categories():
    print("Route /get_admin_categories was called")
    db = g.db
    try:
        clerk_id = request.headers.get('Clerk-User-Id')

        user = get_user_by_clerk_id(db, clerk_id)

        if not user:
            return jsonify({"error": "Missing user_id"}), 400

        categories = get_categories_for_admin_user(db, user.id)
        print(f"categories fetched: {len(categories)}")

        results = [join_org_and_to_dict(db, category.id) for category in categories]
        print("results ready:", results)

        return jsonify(results), 200

    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@users_bp.route("/get_role", methods=["GET"])
def get_user_role():
    db = g.db
    try:
        clerk_id = request.headers.get('Clerk-User-Id')
        if not clerk_id:
            return jsonify({"error": "Missing clerk_id"}), 400
        
        user = get_user_by_clerk_id(db, clerk_id)
        if user is None:
            return jsonify({"error": "User not found"}), 404
        
        is_manager, is_admin, role_orgs = get_role(db, user.id)

        return jsonify({
            "is_manager": is_manager,
            "is_admin": is_admin,
            "roles": [{"role": role, "org_id": org_id} for role, org_id in role_orgs]
        }), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500
