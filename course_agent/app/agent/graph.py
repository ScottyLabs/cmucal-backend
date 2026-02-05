# course-agent/app/agent/graph.py
from langgraph.graph import StateGraph, END
from course_agent.app.agent.state import CourseAgentState
from course_agent.app.agent.nodes.search import search_node
from course_agent.app.agent.nodes.verify_site import verify_site_node
from course_agent.app.agent.nodes.critic import critic_node
from course_agent.app.agent.nodes.extract_calendar import extract_calendar_node
from course_agent.app.agent.router import route_after_verify, route_after_critic

def build_course_agent():
    graph = StateGraph(CourseAgentState)

    graph.add_node("search", search_node)
    graph.add_node("verify_site", verify_site_node)
    graph.add_node("critic", critic_node)
    graph.add_node("extract_calendar", extract_calendar_node)

    graph.set_entry_point("search")

    graph.add_edge("search", "verify_site")

    graph.add_conditional_edges(
        "verify_site",
        route_after_verify,
        {
            "verify_site": "verify_site",
            "critic": "critic",
            "end": END
        }
    )

    graph.add_conditional_edges(
        "critic",
        route_after_critic,
        {
            "verify_site": "verify_site",
            "extract_calendar": "extract_calendar",
            "end": END
        }
    )

    graph.add_edge("extract_calendar", END)

    return graph.compile()
