# course-agent/app/agent/nodes/extract_calendar.py
from course_agent.app.services.iframe_scanner import find_google_calendar_iframe, derive_ical_link
from course_agent.app.services.html_fetcher import crawl_site_pages
# from course_agent.app.db.repositories import insert_calendar
from course_agent.app.agent.state import CourseAgentState
from course_agent.app.db.repositories import upsert_calendar_source

def extract_calendar_node(state: CourseAgentState):
    html = state.get("verified_site_html")
    verified_site_url = state.get("verified_site_url")
    org_id = state.get("org_id")
    category_id = state.get('category_id')
    website_id = state.get('verified_site_id')
    pages_scanned = 0

    print(f"Extracting calendar for course {state.get('course_number')} with org_id {org_id}, category_id {category_id}, website_id {website_id}, html length {len(html) if html else 'None'}")
    if not html or not org_id or not category_id or not website_id:
        return {
            **state,
            "terminal_status": "no_site_found",
            "pages_scanned": pages_scanned,
            "done": True,
        }

    pages_scanned += 1
    calendar_page_url = verified_site_url
    iframe_url = find_google_calendar_iframe(html)

    # Homepage-first, then bounded site crawl fallback.
    if not iframe_url and verified_site_url:
        crawled_pages = crawl_site_pages(
            start_url=verified_site_url,
            max_pages=25,
            max_depth=2,
        )
        for page in crawled_pages:
            page_url = str(page["url"])
            page_depth = int(page["depth"])
            page_html = str(page["html"])

            # Homepage was already scanned using verified_site_html.
            if page_depth == 0:
                continue

            pages_scanned += 1
            iframe_url = find_google_calendar_iframe(page_html)
            if iframe_url:
                calendar_page_url = page_url
                break

    if not iframe_url:
        return {
            **state,
            "terminal_status": "no_calendar",
            "pages_scanned": pages_scanned,
            "done": True,
        }

    ical_link = derive_ical_link(iframe_url)
    if not ical_link:
        return {
            **state,
            'terminal_status': 'no_calendar',
            'calendar_page_url': calendar_page_url,
            'pages_scanned': pages_scanned,
            'done': True,
        }
    
    # skip DB writes if already seen this ical_link
    if state.get('ical_link') == ical_link:
        print(f"Skipping DB write for already seen ical_link: {ical_link}")
        return {
            **state,
            'iframe_url': iframe_url,
            'ical_link': ical_link,
            'calendar_page_url': calendar_page_url,
            'pages_scanned': pages_scanned,
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
        "calendar_page_url": calendar_page_url,
        "pages_scanned": pages_scanned,
        "terminal_status": "success",
        "done": True,
    }