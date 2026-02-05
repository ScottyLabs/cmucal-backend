# tests/monitors/test_schedule_of_classes.py
import pathlib
from scraper.monitors.academic.schedule_of_classes import ScheduleOfClassesScraper

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def parse_fixture(name: str):
    scraper = ScheduleOfClassesScraper(db=None, semester_label="Fall_26")
    html = load_fixture(name)
    fixed_html = scraper._fix_malformed_html(html)
    return scraper._parse_html(fixed_html)


def test_architecture_includes_48026():
    resources = parse_fixture("soc_architecture.html")
    course_nums = {r.course_num for r in resources}

    assert "48026" in course_nums
    assert "48105" in course_nums


def test_architecture_has_multiple_events_for_48105():
    resources = parse_fixture("soc_architecture.html")

    events_48105 = [r for r in resources if r.course_num == "48105"]

    # Lec + recitations
    assert len(events_48105) == 6
    assert {e.lecture_section for e in events_48105} == {"Lec", "A", "B", "C", "D", "E"}

def test_stagecraft_multiple_events():
    resources = parse_fixture("soc_stage.html")

    events = [r for r in resources if r.course_num == "54152"]
    # count D3 events in events.lecture_section
    d3_events = [e for e in events if e.lecture_section == "D3"]
    assert len(d3_events) == 3
    assert all(e.course_name == "Stagecraft: Rigging (3 units)" for e in d3_events)

def test_no_tba_or_empty_times():
    resources = parse_fixture("soc_architecture.html")

    for r in resources:
        assert r.lecture_days != "TBA"
        assert r.lecture_time_start
        assert r.lecture_time_end


def test_all_course_nums_are_numeric():
    resources = parse_fixture("soc_architecture.html")

    for r in resources:
        assert r.course_num.isdigit()

def test_semester_is_attached():
    resources = parse_fixture("soc_architecture.html")

    for r in resources:
        assert r.semester is not None
        assert "_" in r.semester  # e.g. Fall_26
