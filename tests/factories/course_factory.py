import pytest


@pytest.fixture
def course_factory():
    def create_course(
        *,
        id="1",
        org,
        course_number="15213",
        course_name="Intro to Computer Systems",
        **kwargs
    ):
        return {
            "id": id,
            "org_id": org.id,
            "course_number": course_number,
            "course_name": course_name,
            **kwargs,
        }

    return create_course

@pytest.fixture
def course_batch_factory(course_factory, org_factory):
    def create_batch(n=3):
        org = org_factory(name="CMU")
        return [
            course_factory(
                id=str(i),
                org=org,
                course_number=f"15{200 + i}",
                course_name=f"Course {i}",
                agent_run_id='test-run',
            )
            for i in range(n)
        ]
    return create_batch

