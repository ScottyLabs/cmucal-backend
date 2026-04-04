VERIFIER_SCORES = {
    "yes": 0.9,
    "maybe": 0.6,
    "no": 0.2
}

CRITIC_SCORES = {
    "accept": 0.9,
    "reject": 0.1
}

def heuristic_score(url: str, html: str) -> float:
    score = 0.0

    if "cmu.edu" in url:
        score += 0.4

    if any(x in html.lower() for x in ["syllabus", "schedule", "lectures", "office hours"]):
        score += 0.3

    if any(x in url.lower() for x in ["piazza", "canvas", "reddit"]):
        score -= 0.5

    return max(0.0, min(score, 1.0))


def apply_semester_match_boost(
    base_score: float,
    semester_decision: str | None,
    boost: float = 0.1,
) -> float:
    """Apply boost-only semester adjustment.

    - Explicit match: add a small boost.
    - Ambiguous or mismatch: no penalty here.
      (Mismatch is handled by strict rejection upstream.)
    """
    adjusted = base_score
    if semester_decision == "match":
        adjusted += boost
    return max(0.0, min(adjusted, 1.0))
