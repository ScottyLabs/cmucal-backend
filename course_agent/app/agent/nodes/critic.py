# course-agent/app/agent/nodes/critic.py
from course_agent.app.services.llm import get_llm
from course_agent.app.agent.prompts import CRITIC_PROMPT
from course_agent.app.db.repositories import verify_course_website
from course_agent.app.agent.state import CourseAgentState
from course_agent.app.agent.scores import CRITIC_SCORES

llm = None

def critic_node(state: CourseAgentState):
    website_id = state.get("proposed_site_id")
    url = state.get("proposed_site_url")
    html = state.get("proposed_site_html")

    if not website_id or not url or not html:
        print(f"No proposed site to critique in critic_node: website_id={website_id}, url={url is not None}, html_length={len(html) if html else 'N/A'}")
        return {
            **state,
            "done": True,
            "terminal_status": "no_site_found",
        }

    # confidence threshold shortcut
    if (
        state["verifier_score"] >= 0.9 and
        state["heuristic_score"] >= 0.7
    ):
        verify_course_website(
            website_id,
            "Auto-approved (high confidence)",
            final_score=0.9
        )
        return {
            **state,
            "critic_decision": "accept",
            "verified_site_id": website_id,
            "verified_site_url": url,
            "verified_site_html": html,
            "critic_score": 0.9,
            "final_score": 0.9,
            "terminal_status": "success",
            "done": False
        }

    global llm
    if llm is None:
        llm = get_llm()

    snippet = html[:1500]

    formatted_course_name = f"{state['course_number'][0:2]}-{state['course_number'][2:4]} {state['course_name']}"
    decision = llm.invoke(
        CRITIC_PROMPT.format(
            course_name=formatted_course_name,
            url=url,
            text=snippet
        )
    ).content.strip().lower()

    critic_score = CRITIC_SCORES.get(decision, 0.1)

    final_score = (
        0.5 * state["verifier_score"] +
        0.3 * critic_score +
        0.2 * state["heuristic_score"]
    )

    print(f"Critic decision: {decision}, Critic score: {critic_score}, Final score: {final_score}")
    if decision == "accept":
        verify_course_website(
            website_id,
            f"Approved by CriticAgent; score={final_score:.2f}",
            final_score
        )
        return {
            **state,
            "critic_decision": decision,
            "verified_site_id": website_id,
            "verified_site_url": url,
            "verified_site_html": html,
            "critic_score": critic_score,
            "final_score": final_score,
            "terminal_status": "success",
            "done": False
        }

    # reject path
    return {
        **state,
        "critic_decision": decision,
        "critic_score": critic_score,
        "final_score": final_score,
        "current_url_index": state["current_url_index"] + 1,
        "proposed_site_id": None,
        "proposed_site_url": None,
        "proposed_site_html": None,
        "done": False,
    }


