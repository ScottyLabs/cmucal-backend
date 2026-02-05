from course_agent.app.agent.graph import build_course_agent

def test_agent_runs_multiple_courses(
    mocker,
    course_batch_factory,
):
    from course_agent.app.agent.graph import build_course_agent

    courses = course_batch_factory(3)
    agent = build_course_agent()

    mocker.patch(
        'course_agent.app.agent.nodes.search.get_search_course_site',
        return_value=lambda *_args, **_kwargs: [
            'https://www.cs.cmu.edu/~213', 'https://cmu-313.github.io/'
        ]
    )


    mocker.patch(
        'course_agent.app.services.html_fetcher.fetch_html',
        return_value='<html>syllabus</html>'
    )

    mock_llm = mocker.Mock()
    mock_llm.invoke.side_effect = (
        [type('R', (), {'content': 'yes'}),
         type('R', (), {'content': 'accept'})] * 3
    )

    mocker.patch(
        'course_agent.app.agent.nodes.verify_site.upsert_course_website',
        return_value='site-x'
    )
    mocker.patch(
        'course_agent.app.agent.nodes.critic.verify_course_website'
    )
    mocker.patch(
        'course_agent.app.agent.nodes.extract_calendar.upsert_calendar_source'
    )
    mocker.patch(
        'course_agent.app.agent.nodes.verify_site.get_course_website_by_url',
        return_value=None
    )
    mocker.patch(
        'course_agent.app.agent.nodes.verify_site.llm',
        mock_llm
    )
    mocker.patch(
        'course_agent.app.agent.nodes.critic.llm',
        mock_llm
    )


    for course in courses:
        state = agent.invoke({
            **course,
            'course_id': course['id'],
            "category_id": "cat-test",
            'candidate_urls': [],
            'current_url_index': 0,
            'verified_site_id': None,
            'iframe_url': None,
            'ical_link': None,
            'terminal_status': None,
            'done': False,
            'event_type': 'ACADEMIC',
        })
        print(state['terminal_status'])
        assert state['verified_site_id'] == 'site-x'
