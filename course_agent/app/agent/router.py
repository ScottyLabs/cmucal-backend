# course-agent/app/agent/router.py
from course_agent.app.agent.state import CourseAgentState

def route_after_verify(state: CourseAgentState):
    if state.get("proposed_site_id"):
        print("Routing to critic node")
        return "critic"
    if state.get("done"):
        print("Routing to end from verify_site")
        return "end"
    return "verify_site"

def route_after_critic(state: CourseAgentState):
    if state.get("verified_site_id"):
        print("Routing to extract_calendar node")
        return "extract_calendar"
    if state.get("done"):
        print("Routing to end from critic node")
        return "end"
    return "verify_site"