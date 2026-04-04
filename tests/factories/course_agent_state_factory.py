import pytest

@pytest.fixture
def ca_base_state():
    return {
        'course_id': 1,
        'org_id': 10,
        'category_id': 20,
        'course_number': '15213',
        'course_name': 'Intro to Computer Systems',
        'agent_run_id': 'run-1',
        'event_type': 'ACADEMIC',

        'candidate_urls': [],
        'current_url_index': 0,

        'proposed_site_id': None,
        'proposed_site_url': None,
        'proposed_site_html': None,

        'verified_site_id': None,
        'verified_site_url': None,
        'iframe_url': None,
        'ical_link': None,
        'calendar_page_url': None,
        'pages_scanned': None,

        'verifier_score': None,
        'critic_score': None,
        'heuristic_score': None,
        'expected_semester': None,
        'semester_relevance_score': None,
        'semester_reason': None,
        'semester_decision': None,
        'semester_matched_terms': None,
        'final_score': None,

        'terminal_status': None,
        'done': False,
    }
