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
