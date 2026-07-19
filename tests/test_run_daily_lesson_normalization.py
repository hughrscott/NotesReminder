import pytest

pd = pytest.importorskip("pandas")

from run_daily import should_skip_lesson  # noqa: E402


def test_should_skip_lesson_accepts_nan_instructor_and_lesson_type():
    nan = float("nan")
    assert should_skip_lesson("Guitar Lesson", "Student Name", nan) is False
    assert should_skip_lesson(nan, "Student Name", nan) is False


def test_should_skip_lesson_still_filters_admin_and_non_person_instructors():
    assert should_skip_lesson("Admin Meeting", "", "Teacher") is True
    assert should_skip_lesson("Guitar Lesson", "Student Name", "---") is True
