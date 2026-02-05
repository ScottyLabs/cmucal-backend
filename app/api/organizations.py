from flask import Blueprint, jsonify, request, g
from app.models.user import get_user_by_clerk_id, get_user_by_email, create_user_without_clerk, get_user_by_id
from app.models.organization import create_organization, get_orgs_by_type, get_organization_by_name, get_organization_by_id
from app.models.models import Organization, Category, Event, EventOccurrence
from app.models.admin import create_admin, get_admin_by_org_and_user, get_admins_by_org
from app.models.category import create_category, get_categories_by_org_id
from app.services.ical import delete_events_for_calendar_source
from app.utils.course_data import get_course_data
from app.utils.auth import get_current_user


orgs_bp = Blueprint("orgs", __name__)

def event_occurrence_to_dict(occurrence: EventOccurrence):
    """Manually serialize EventOccurrence SQLAlchemy object to a dictionary."""
    return {
        "id": occurrence.id,
        "title": occurrence.title,
        "description": occurrence.description,
        "start_datetime": occurrence.start_datetime.isoformat(),
        "end_datetime": occurrence.end_datetime.isoformat(),
        "location": occurrence.location,
        "is_all_day": occurrence.is_all_day,
        "source_url": occurrence.source_url,
        "recurrence": occurrence.recurrence.name if occurrence.recurrence else None,
        "event_id": occurrence.event_id,
        "org_id": occurrence.org_id,
        "category_id": occurrence.category_id,
    }

@orgs_bp.route("/org/<int:org_id>", methods=['GET'])
def get_organization_data(org_id):
    """returns a single organization's data with its categories and event occurrences"""
    clerk_user_id = request.headers.get('Clerk-User-Id')
    user = get_current_user(clerk_user_id)

    if not user:
        return jsonify({"error": "User not found"}), 404

    db = g.db
    try:
        org = db.query(Organization).filter(Organization.id == org_id).first()
        if not org:
            return jsonify({"error": "Organization not found"}), 404

        org_data = {
            "org_id": org.id,
            "name": org.name,
            "type": org.type,
            "categories": [],
            "events": {}
        }

        # Get all categories for this org
        categories = db.query(Category).filter(Category.org_id == org.id).all()
        
        # Add categories and their events
        for category in categories:
            org_data["categories"].append({
                "id": category.id,
                "name": category.name
            })
            
            # Get events for this category
            events = db.query(Event).filter(
                Event.org_id == org.id,
                Event.category_id == category.id
            ).all()
            
            # Get event occurrences
            event_ids = [e.id for e in events]
            occurrences = []
            if event_ids:
                occurrences = db.query(EventOccurrence).filter(
                    EventOccurrence.event_id.in_(event_ids)
                ).all()
            
            org_data["events"][category.name] = [event_occurrence_to_dict(o) for o in occurrences]
            
        return jsonify(org_data)

    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/get_all_orgs", methods=["GET"])
def get_all_orgs():
    db = g.db
    try:
        orgs = db.query(Organization).all()
        orgs_list = []
        for org in orgs:
            orgs_list.append({
                "id": org.id,
                "name": org.name,
                "description": org.description,
                "type": org.type,
                "tags": org.tags,
            })

        return jsonify(orgs_list), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/get_course_orgs", methods=["GET"])
def get_course_orgs():
    db = g.db
    try:
        orgs = get_orgs_by_type(db, org_type='COURSE')
        print(f"Found {len(orgs)} COURSE organizations")
        orgs_list = []
        for org in orgs:
            parts = org.name.split(" ")
            course_num = parts[0]
            course_title = " ".join(parts[1:])
            orgs_list.append({
                "id": org.id,
                "number": course_num,
                "title": course_title,
                "label": org.name,
            })

        return jsonify(orgs_list), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/get_club_orgs", methods=["GET"])
def get_club_orgs():
    db = g.db
    try:
        # Debug: Check all organizations first
        all_orgs = db.query(Organization).all()
        print(f"Total organizations in database: {len(all_orgs)}")
        for org in all_orgs[:5]:  # Print first 5 for debugging
            print(f"Org ID: {org.id}, Name: {org.name}, Type: {org.type}")
        
        orgs = get_orgs_by_type(db, org_type='CLUB')
        print(f"Found {len(orgs)} CLUB organizations")
        
        if not orgs:
            # Return empty list instead of 404 for better UX
            return jsonify([]), 200
            
        orgs_list = []
        for org in orgs:
            orgs_list.append({
                "id": org.id,
                "name": org.name,
                "description": org.description,
            })

        return jsonify(orgs_list), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/get_courses", methods=["GET"])
def get_courses_from_soc():
    """
    Endpoint to fetch course data from the JSON file.
    To update the JSON file, follow the instructions in the README in the rust directory.
    """
    try:
        courses = get_course_data()
        return jsonify(courses), 200
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 404
    except Exception as e:
        return jsonify({"error": "An error occurred while fetching course data."}), 500

@orgs_bp.route("/create_org", methods=["POST"])
def create_org_record():
    db = g.db
    try:
        data = request.get_json()
        org_name = data.get("name")
        org_description = data.get("description", None)
        org_type = data.get("type", None)
        if not org_name:
            return jsonify({"error": "Missing org_name"}), 400

        org = create_organization(db, name=org_name, description=org_description, type=org_type)
        db.commit()
        return jsonify({"status": "created", "org_id": org.id}), 201
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/create_category", methods=["POST"])
def create_category_record():
    db = g.db
    try:
        data = request.get_json()
        org_id = data.get("org_id")
        if not org_id:
            return jsonify({"error": "Missing org_id"}), 400
        name = data.get("name")
        if not name:
            return jsonify({"error": "Missing category name"}), 400
        
        category = create_category(db, org_id=org_id, name=name)
        db.commit()
        return jsonify({"status": "category created", "category_id": category.id}), 201
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@orgs_bp.route(
    "/<int:org_id>/calendar-sources/<int:calendar_source_id>/events",
    methods=["DELETE"]
)
def delete_events_and_deactivate_calendar(org_id: int, calendar_source_id: int):
    """
    Deletes all events associated with a calendar source
    and deactivates the calendar source for the given org.

    curl -X DELETE \
    http://localhost:5001/api/organizations/<org_id>/calendar-sources/<calendar_source_id>/events
    """
    db = g.db
    try:
        deleted_event_ids = delete_events_for_calendar_source(
            db=db,
            calendar_source_id=calendar_source_id,
        )

        db.commit()

        return jsonify({
            "status": "ok",
            "org_id": org_id,
            "calendar_source_id": calendar_source_id,
            "deleted_events": len(deleted_event_ids),
            "event_ids": deleted_event_ids,
        }), 200

    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": "Internal server error"}), 500

@orgs_bp.route("/create_test_clubs", methods=["POST"])
def create_test_clubs():
    """Create some test club organizations for development"""
    db = g.db
    try:
        test_clubs = [
            {"name": "ScottyLabs", "description": "A community of passionate, interdisciplinary leaders that use design and technology to achieve more."},
            {"name": "UXA", "description": "User Experience Association - Exploring the intersection of design and technology"},
            {"name": "Activities Board", "description": "Programming events and activities for the CMU community"},
            {"name": "Badminton Club", "description": "CMU Badminton Club for recreational and competitive play"},
            {"name": "Robotics Club", "description": "Building and programming robots for competitions and fun"},
            {"name": "Photography Club", "description": "Capturing moments and exploring creative photography"}
        ]
        
        created_clubs = []
        for club_data in test_clubs:
            # Check if club already exists
            existing = db.query(Organization).filter(
                Organization.name == club_data["name"],
                Organization.type == "CLUB"
            ).first()
            
            if not existing:
                org = create_organization(db, 
                                        name=club_data["name"], 
                                        description=club_data["description"], 
                                        type="CLUB")
                created_clubs.append(org.name)
                db.commit()
        return jsonify({
            "status": "success", 
            "created_clubs": created_clubs,
            "message": f"Created {len(created_clubs)} new clubs"
        }), 201
        
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/create_admin", methods=["POST"])
def create_admin_record():
    db = g.db
    try:
        data = request.get_json()
        user_id = data.get("user_id")
        if not user_id:
            return jsonify({"error": "Missing user_id"}), 400
        org_id = data.get("org_id")
        if not org_id:
            return jsonify({"error": "Missing org_id"}), 400
        role = data.get("role", "admin")
        category_id = data.get("category_id", None)
        
        
        admin = create_admin(db, org_id=org_id, user_id=user_id, role=role, category_id=category_id)
        db.commit()
        return jsonify({"status": "admin created", "user": admin.user_id, "org": admin.org_id}), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/bulk_create_admins", methods=["POST"])
def bulk_create_admins():
    """
    Create multiple users and assign them as admins to organizations.
    
    Expected payload:
    {
        "user_emails": "email1@example.com,email2@example.com,email3@example.com",
        "organization_name": "ScottyLabs"
    }
    """
    db = g.db
    try:
        data = request.get_json()
        user_emails_str = data.get("user_emails")
        organization_name = data.get("organization_name")
        
        if not user_emails_str or not organization_name:
            return jsonify({"error": "Missing user_emails or organization_name"}), 400
        
        # Parse comma-separated emails
        user_emails = [email.strip() for email in user_emails_str.split(",") if email.strip()]
        
        if not user_emails:
            return jsonify({"error": "No valid emails provided"}), 400
        
        # Find or create organization
        organization = get_organization_by_name(db, organization_name)
        if not organization:
            # Create new organization
            organization = create_organization(db, name=organization_name, type="CLUB")
        
        # Get or create categories for this organization
        categories = get_categories_by_org_id(db, organization.id)
        if not categories:
            # Create default "Main" category
            main_category = create_category(db, org_id=organization.id, name="Main")
            categories = [main_category]
        
        created_users = []
        created_admins = []
        errors = []
        
        for email in user_emails:
            try:
                # Find or create user
                user = get_user_by_email(db, email)
                if not user:
                    # Create new user without clerk_id
                    user = create_user_without_clerk(db, email=email)
                    created_users.append(user.email)
                
                # Check if admin relationship already exists
                existing_admin = get_admin_by_org_and_user(db, organization.id, user.id)
                if existing_admin:
                    # Update existing admin with category if needed
                    if not existing_admin.category_id and categories:
                        existing_admin.category_id = categories[0].id
                        db.add(existing_admin)
                        db.commit()
                    continue
                
                # Create admin relationship
                category_id = categories[0].id if categories else None
                admin = create_admin(
                    db, 
                    org_id=organization.id, 
                    user_id=user.id, 
                    role="admin",
                    category_id=category_id
                )
                created_admins.append({
                    "user_email": user.email,
                    "user_id": user.id,
                    "org_id": organization.id,
                    "category_id": category_id
                })
                
            except Exception as e:
                errors.append(f"Error processing {email}: {str(e)}")
                continue
        
        response_data = {
            "status": "success",
            "organization": {
                "id": organization.id,
                "name": organization.name
            },
            "categories": [{"id": cat.id, "name": cat.name} for cat in categories],
            "created_users": created_users,
            "created_admins": created_admins,
            "errors": errors
        }

        db.commit()
        return jsonify(response_data), 201
            
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/get_admins_in_org", methods=["GET"])
def get_admins_in_org():
    db = g.db
    try:
        org_id = request.args.get("org_id")
        if not org_id:
            return jsonify({"error": "Missing org_id"}), 400
        
        admins = get_admins_by_org(db, org_id=int(org_id))
        
        admins_list = []
        for admin in admins:
            user = get_user_by_id(db, admin.user_id)
            org = get_organization_by_id(db, admin.org_id)
            andrew_id = user.email.split("@")[0] if user.email else "N/A"
            print(f"Admin User: {andrew_id}, Org: {org.name}, Role: {admin.role}")
            admins_list.append({
                "user_id": user.id,
                "andrew_id": andrew_id,
                "user_email": user.email,
                "org_id": org.id,
                "org_name": org.name,
                "role": admin.role,
                "category_id": admin.category_id
            })
        
        return jsonify(admins_list), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@orgs_bp.route("/get_user_role_in_org", methods=["GET"])
def get_user_role_in_org():
    db = g.db
    try:
        clerk_id = request.headers.get('Clerk-User-Id')
        if not clerk_id:
            return jsonify({"error": "Missing clerk_id"}), 400
        user = get_user_by_clerk_id(db, clerk_id)
        if user is None:
            return jsonify({"error": "User not found"}), 404
        
        org_id = request.args.get("org_id")
        if not org_id:
            return jsonify({"error": "Missing org_id"}), 400
        
        admin = get_admin_by_org_and_user(db, org_id=int(org_id), user_id=int(user.id))
        if not admin:
            return jsonify({"role": "member"}), 200
        
        return jsonify({"role": admin.role}), 200
    except Exception as e:
        import traceback
        print("❌ Exception:", traceback.format_exc())
        return jsonify({"error": str(e)}), 500

