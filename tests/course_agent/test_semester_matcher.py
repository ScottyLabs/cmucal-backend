from datetime import date


from course_agent.app.services.semester_matcher import (
    evaluate_semester_relevance,
    infer_expected_semester,
    resolve_expected_semester,
)


def test_infer_expected_semester_boundaries():
    assert infer_expected_semester(date(2026, 1, 10)) == "Spring 2026"
    assert infer_expected_semester(date(2026, 5, 10)) == "Spring 2026"

    assert infer_expected_semester(date(2026, 5, 11)) == "Summer 2026"
    assert infer_expected_semester(date(2026, 8, 9)) == "Summer 2026"

    assert infer_expected_semester(date(2026, 8, 10)) == "Fall 2026"
    assert infer_expected_semester(date(2026, 12, 15)) == "Fall 2026"

    assert infer_expected_semester(date(2026, 12, 16)) == "Spring 2027"
    assert infer_expected_semester(date(2026, 1, 9)) == "Spring 2026"


def test_resolve_expected_semester_override():
    assert resolve_expected_semester("spring 2026") == "Spring 2026"


def test_evaluate_semester_match_and_shorthand():
    result = evaluate_semester_relevance(
        "This page contains S'26 lecture schedule and assignments.",
        "Spring 2026",
    )
    assert result["decision"] == "match"



def test_evaluate_semester_mismatch_explicit_term_year():
    result = evaluate_semester_relevance(
        "Official course page for Fall 2025.",
        "Spring 2026",
    )
    assert result["decision"] == "mismatch"
    assert result["score"] == 0.0


def test_evaluate_semester_mini_mapping_match():
    result = evaluate_semester_relevance(
        "Course website for Mini 3 second-half content.",
        "Spring 2026",
    )
    assert result["decision"] == "match"


def test_evaluate_semester_mini_mapping_mismatch():
    result = evaluate_semester_relevance(
        "Course website for Mini 1.",
        "Spring 2026",
    )
    assert result["decision"] == "mismatch"


def test_evaluate_semester_ambiguous_is_neutral():
    result = evaluate_semester_relevance(
        "Course website with syllabus and assignments but no term mention.",
        "Spring 2026",
    )
    assert result["decision"] == "ambiguous"
    assert result["score"] == 0.5


def test_verify_site_rejects_explicit_semester_mismatch(mocker, ca_base_state):
    from course_agent.app.agent.nodes.verify_site import verify_site_node

    state = {
        **ca_base_state,
        "candidate_urls": ["https://example.com/course"],
        "current_url_index": 0,
    }

    mocker.patch(
        "course_agent.app.agent.nodes.verify_site.fetch_html",
        return_value="Fall 2025 schedule and syllabus",
    )
    upsert = mocker.patch(
        "course_agent.app.agent.nodes.verify_site.upsert_course_website",
        return_value="site-x",
    )
    mocker.patch(
        "course_agent.app.agent.nodes.verify_site.get_target_semester_override",
        return_value="Spring 2026",
    )

    # Avoid attempting a real LLM initialization before early mismatch return.
    mocker.patch("course_agent.app.agent.nodes.verify_site.llm", mocker.Mock())

    out = verify_site_node(state)

    assert out["done"] is False
    assert out["current_url_index"] == 1
    assert out["semester_decision"] == "mismatch"
    assert out["expected_semester"] == "Spring 2026"
    upsert.assert_called_once()


def test_verify_site_keeps_ambiguous_neutral(mocker, ca_base_state):
    from course_agent.app.agent.nodes.verify_site import verify_site_node

    state = {
        **ca_base_state,
        "candidate_urls": ["https://example.com/course"],
        "current_url_index": 0,
    }

    mocker.patch(
        "course_agent.app.agent.nodes.verify_site.fetch_html",
        return_value="Syllabus and assignments page with no explicit semester.",
    )
    mocker.patch(
        "course_agent.app.agent.nodes.verify_site.get_target_semester_override",
        return_value="Spring 2026",
    )
    mocker.patch(
        "course_agent.app.agent.nodes.verify_site.get_course_website_by_url",
        return_value=None,
    )
    mocker.patch(
        "course_agent.app.agent.nodes.verify_site.upsert_course_website",
        return_value="site-1",
    )

    mock_llm = mocker.Mock()
    mock_llm.invoke.return_value = type("R", (), {"content": "yes"})
    mocker.patch("course_agent.app.agent.nodes.verify_site.llm", mock_llm)

    out = verify_site_node(state)

    assert out["done"] is False
    assert out["proposed_site_id"] == "site-1"
    assert out["semester_decision"] == "ambiguous"
    assert out["semester_relevance_score"] == 0.5
