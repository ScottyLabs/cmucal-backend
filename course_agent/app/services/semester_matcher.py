import re
from datetime import date
from typing import Any


TERM_ALIASES = {
    "spring": "spring",
    "s": "spring",
    "summer": "summer",
    "fall": "fall",
    "autumn": "fall",
    "f": "fall",
}


# Explicit mini mapping provided by product requirements.
MINI_TO_TERM = {
    1: "fall",
    2: "fall",
    3: "spring",
    4: "spring",
    5: "summer",
    6: "summer",
}


TERM_YEAR_RE = re.compile(
    r"\b(spring|summer|fall|autumn|f|s)\s*[-_/]?\s*['\"“”’]?\s*(20\d{2}|\d{2})\b",
    re.IGNORECASE,
)
MINI_RE = re.compile(r"\bmini\s*([1-6])\b", re.IGNORECASE)


def _normalize_year(raw_year: str) -> int:
    year = int(raw_year)
    if year >= 100:
        return year
    # Assume modern course years for shorthand (e.g., 26 -> 2026).
    return 2000 + year


def _parse_expected_semester(expected_semester: str) -> tuple[str, int]:
    match = re.search(r"\b(spring|summer|fall)\s+(20\d{2})\b", expected_semester.strip(), re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid expected semester format: {expected_semester}")
    return match.group(1).lower(), int(match.group(2))


def infer_expected_semester(today: date | None = None) -> str:
    """Infer semester from fixed boundary windows.

    Windows used:
    - Spring: Jan 9 to May 20 
    - Summer: May 11 to Aug 9
    - Fall: Aug 10 to Dec 15

    Dates outside these windows are mapped to the nearest upcoming semester:
    - Dec 16 to Jan 9 -> Spring (next/current year)
    """
    today = today or date.today()
    mmdd = (today.month, today.day)

    if (1, 10) <= mmdd <= (5, 10):
        return f"Spring {today.year}"
    if (5, 11) <= mmdd <= (8, 9):
        return f"Summer {today.year}"
    if (8, 10) <= mmdd <= (12, 15):
        return f"Fall {today.year}"

    if mmdd >= (12, 16):
        return f"Spring {today.year + 1}"
    return f"Spring {today.year}"


def resolve_expected_semester(override: str | None, today: date | None = None) -> str:
    if override and override.strip():
        term, year = _parse_expected_semester(override)
        return f"{term.capitalize()} {year}"
    return infer_expected_semester(today=today)


def evaluate_semester_relevance(text: str, expected_semester: str) -> dict[str, Any]:
    """Evaluate whether a page explicitly matches the expected semester.

    Returns a dict with fields:
    - decision: one of "match", "mismatch", "ambiguous"
    - score: float in [0.0, 1.0]
    - reason: short explanation for logs/debug
    - matched_terms: list of detected semester signals
    """
    expected_term, expected_year = _parse_expected_semester(expected_semester)
    lowered_text = text.lower()

    matched_terms: list[str] = []
    explicit_term_year: list[tuple[str, int, str]] = []

    for match in TERM_YEAR_RE.finditer(lowered_text):
        raw_term = match.group(1).lower()
        raw_year = match.group(2)
        term = TERM_ALIASES.get(raw_term)
        if not term:
            continue
        year = _normalize_year(raw_year)
        raw = match.group(0)
        explicit_term_year.append((term, year, raw))
        matched_terms.append(raw)

    mini_terms: list[tuple[str, str]] = []
    for match in MINI_RE.finditer(lowered_text):
        mini_number = int(match.group(1))
        mapped_term = MINI_TO_TERM[mini_number]
        raw = match.group(0)
        mini_terms.append((mapped_term, raw))
        matched_terms.append(raw)

    # Strongest signal: explicit term + year pairs.
    if explicit_term_year:
        has_exact_match = any(
            term == expected_term and year == expected_year
            for term, year, _ in explicit_term_year
        )
        if has_exact_match:
            return {
                "decision": "match",
                "score": 1.0,
                "reason": f"Explicit semester/year match for {expected_semester}",
                "matched_terms": matched_terms,
            }

        return {
            "decision": "mismatch",
            "score": 0.0,
            "reason": f"Explicit semester/year does not match {expected_semester}",
            "matched_terms": matched_terms,
        }

    # Secondary signal: mini-term mapping.
    if mini_terms:
        if any(term == expected_term for term, _ in mini_terms):
            return {
                "decision": "match",
                "score": 0.9,
                "reason": f"Mini-term mapping aligns with {expected_term}",
                "matched_terms": matched_terms,
            }
        return {
            "decision": "mismatch",
            "score": 0.0,
            "reason": f"Mini-term mapping conflicts with {expected_term}",
            "matched_terms": matched_terms,
        }

    # No explicit term/year evidence -> neutral by policy.
    return {
        "decision": "ambiguous",
        "score": 0.5,
        "reason": "No explicit semester/year evidence on page",
        "matched_terms": matched_terms,
    }
