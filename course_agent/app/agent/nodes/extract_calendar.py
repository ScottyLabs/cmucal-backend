# course-agent/app/agent/nodes/extract_calendar.py
from course_agent.app.services.iframe_scanner import find_google_calendar_iframe, derive_ical_link
# from course_agent.app.db.repositories import insert_calendar
from course_agent.app.agent.state import CourseAgentState
from course_agent.app.db.repositories import upsert_calendar_source

def extract_calendar_node(state: CourseAgentState):
    html = state.get("verified_site_html")
    org_id = state.get("org_id")
    category_id = state.get('category_id')
    website_id = state.get('verified_site_id')

    print(f"Extracting calendar for course {state.get('course_number')} with org_id {org_id}, category_id {category_id}, website_id {website_id}, html length {len(html) if html else 'None'}")
    if not html or not org_id or not category_id or not website_id:
        return {
            **state,
            "terminal_status": "no_site_found",
            "done": True,
        }

    iframe_url = find_google_calendar_iframe(html)

    if not iframe_url:
        return {
            **state,
            "terminal_status": "no_calendar",
            "done": True,
        }

    ical_link = derive_ical_link(iframe_url)
    if not ical_link:
        return {
            **state,
            'terminal_status': 'no_calendar',
            'done': True,
        }
    
    # skip DB writes if already seen this ical_link
    if state.get('ical_link') == ical_link:
        print(f"Skipping DB write for already seen ical_link: {ical_link}")
        return {
            **state,
            'terminal_status': 'success',
            'done': True,
        }

    upsert_calendar_source(
        org_id=org_id,
        category_id=state['category_id'],
        url=ical_link,
        notes='Detected from course website iframe',
        default_event_type=state.get('event_type'),
    )

    return {
        **state,
        "iframe_url": iframe_url,
        "ical_link": ical_link,
        "terminal_status": "success",
        "done": True,
    }