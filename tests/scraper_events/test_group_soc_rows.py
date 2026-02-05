import pytest
from datetime import datetime

from scraper.helpers.event import group_soc_rows
from scraper.models import ScheduleOfClasses


SEM_START = datetime(2026, 1, 12)
SEM_END   = datetime(2026, 5, 5)


def make_soc(
    *,
    course_num="15112",
    course_name="Foundations of Programming",
    lecture_section="A",
    lecture_days="MWF",
    lecture_time_start="09:00AM",
    lecture_time_end="09:50AM",
    location="DH 2210",
    semester="Spring_26",
):
    return ScheduleOfClasses(
        id=1,
        course_num=course_num,
        course_name=course_name,
        lecture_section=lecture_section,
        lecture_days=lecture_days,
        lecture_time_start=lecture_time_start,
        lecture_time_end=lecture_time_end,
        location=location,
        semester=semester,
        sem_start=SEM_START,
        sem_end=SEM_END,
    )

def test_groups_identical_soc_rows():
    soc1 = make_soc()
    soc2 = make_soc()

    grouped = group_soc_rows([soc1, soc2])

    assert len(grouped) == 1
    assert len(next(iter(grouped.values()))) == 2

def test_separates_by_location():
    soc1 = make_soc(location="DH 2210")
    soc2 = make_soc(location="GHC 4401")

    grouped = group_soc_rows([soc1, soc2])

    assert len(grouped) == 2

def test_separates_by_time():
    soc1 = make_soc(lecture_time_start="09:00AM")
    soc2 = make_soc(lecture_time_start="10:00AM")

    grouped = group_soc_rows([soc1, soc2])

    assert len(grouped) == 2

def test_separates_by_section():
    soc1 = make_soc(lecture_section="A")
    soc2 = make_soc(lecture_section="B")

    grouped = group_soc_rows([soc1, soc2])

    assert len(grouped) == 2

def test_separates_by_semester():
    soc1 = make_soc(semester="Spring_26")
    soc2 = make_soc(semester="Fall_26")

    grouped = group_soc_rows([soc1, soc2])

    assert len(grouped) == 2

def test_preserves_all_rows():
    socs = [
        make_soc(),
        make_soc(),
        make_soc(lecture_section="B", lecture_time_start="10:00AM"),
    ]

    grouped = group_soc_rows(socs)

    total = sum(len(v) for v in grouped.values())
    assert total == len(socs)
