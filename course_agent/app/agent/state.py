from typing import TypedDict, Optional, List, Literal

class CourseAgentState(TypedDict):
    course_id: int
    org_id: int
    category_id: int 
    course_number: str
    course_name: str
    agent_run_id: str
    event_type: str

    candidate_urls: List[str]
    current_url_index: int

    proposed_site_id: Optional[str]
    proposed_site_url: Optional[str]
    proposed_site_html: Optional[str]

    verified_site_id: Optional[str]
    verified_site_html: Optional[str]

    iframe_url: Optional[str]
    ical_link: Optional[str]

    critic_decision: Optional[str]  # "accept" | "reject"

    verifier_score: Optional[float]
    critic_score: Optional[float]
    heuristic_score: Optional[float]
    final_score: Optional[float]

    terminal_status: Optional[
        Literal["success", "no_site_found", "no_calendar"]
    ]

    done: bool

