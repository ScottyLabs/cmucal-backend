# course-agent/app/agent/nodes/search.py
from course_agent.app.services.search import get_search_course_site
from course_agent.app.agent.state import CourseAgentState


def search_node(state: CourseAgentState):
    search = get_search_course_site()
    urls = search(
        f"{state['course_number']} {state['course_name']}",
        max_results=3,
    )
    if not urls:
        return {
            **state,
            "candidate_urls": [],
            "done": True,
            "terminal_status": "no_site_found"
        }
    return {
        **state,
        "candidate_urls": urls,
        "current_url_index": 0,
    }
