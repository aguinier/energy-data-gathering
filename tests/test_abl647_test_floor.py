"""ABL-647: the CI gate that answers "did anything RUN?".

pytest's exit code answers "did anything fail?" and nothing else. A mistyped
path argument, a collection error, or a deleted file all leave the command
exiting 0 having run less -- and CI is the one reader that never notices,
because nobody reads a green check. `scripts/test_floor.py` reads the junit
report and asserts the counts; this file asserts the gate.

The skip allowance is the half that is easy to get wrong. A skipped test still
counts in the junit `tests` attribute, so a count floor alone cannot see one.
This repo has a real gated test -- the GEM end-to-end loader needs the xlsx
files under `C:/Code/able/data`, which a runner does not have -- so the
allowance is a measured ceiling rather than zero. What it buys is that
answering a red run with `@pytest.mark.skip` has to move a number in a diff.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "test_floor.py"
_spec = importlib.util.spec_from_file_location("abl647_test_floor", _MODULE_PATH)
assert _spec is not None and _spec.loader is not None
test_floor = importlib.util.module_from_spec(_spec)
sys.modules["abl647_test_floor"] = test_floor
_spec.loader.exec_module(test_floor)


FLOOR = {"tests": 100, "max_skipped": 1}


def _counts(**over: int) -> dict[str, int]:
    base = {"tests": 100, "failures": 0, "errors": 0, "skipped": 0}
    base.update(over)
    return base


def _write_report(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "report.xml"
    path.write_text(body, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# read_junit_counts
# ---------------------------------------------------------------------------


def test_reads_a_testsuites_wrapped_report(tmp_path: Path) -> None:
    """What pytest actually writes: one `<testsuite>` inside `<testsuites>`."""
    report = _write_report(
        tmp_path,
        '<?xml version="1.0" encoding="utf-8"?><testsuites>'
        '<testsuite name="pytest" errors="1" failures="2" skipped="3" tests="40" />'
        "</testsuites>",
    )
    assert test_floor.read_junit_counts(report) == {
        "tests": 40,
        "failures": 2,
        "errors": 1,
        "skipped": 3,
    }


def test_reads_a_bare_testsuite_root(tmp_path: Path) -> None:
    """Also valid junit, and emitted by other tools. Not a shape to fall over on."""
    report = _write_report(
        tmp_path,
        '<testsuite name="pytest" errors="0" failures="0" skipped="1" tests="9" />',
    )
    assert test_floor.read_junit_counts(report)["tests"] == 9


def test_sums_across_multiple_suites(tmp_path: Path) -> None:
    report = _write_report(
        tmp_path,
        "<testsuites>"
        '<testsuite tests="10" failures="1" errors="0" skipped="2" />'
        '<testsuite tests="5" failures="0" errors="1" skipped="0" />'
        "</testsuites>",
    )
    counts = test_floor.read_junit_counts(report)
    assert counts == {"tests": 15, "failures": 1, "errors": 1, "skipped": 2}


def test_missing_attributes_read_as_zero_rather_than_crashing(tmp_path: Path) -> None:
    report = _write_report(tmp_path, '<testsuite tests="3" />')
    assert test_floor.read_junit_counts(report) == {
        "tests": 3,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }


# ---------------------------------------------------------------------------
# evaluate
# ---------------------------------------------------------------------------


def test_accepts_a_run_that_meets_the_floor_exactly() -> None:
    assert test_floor.evaluate(_counts(), FLOOR) == []


def test_accepts_a_run_above_the_floor() -> None:
    """Adding tests must not require bumping the floor first."""
    assert test_floor.evaluate(_counts(tests=140), FLOOR) == []


def test_rejects_a_green_run_that_lost_tests() -> None:
    """The ABL-647 case: nothing failed, so pytest exits 0, and 40 tests are gone."""
    problems = test_floor.evaluate(_counts(tests=60), FLOOR)
    assert len(problems) == 1
    assert "Ran 60 tests; the floor is 100" in problems[0]


def test_rejects_a_run_with_failures_even_when_the_count_is_met() -> None:
    assert "the run failed" in test_floor.evaluate(_counts(failures=1), FLOOR)[0]


def test_rejects_a_collection_error_which_is_an_error_and_not_a_failure() -> None:
    """A file that raises at import reports zero failures and one error.

    This is the shape CLAUDE.md warns about for this repo specifically: run the
    suite on the wrong interpreter and most files report ERROR, not failure.
    """
    assert "1 error(s)" in test_floor.evaluate(_counts(errors=1), FLOOR)[0]


def test_rejects_a_skip_past_the_allowance() -> None:
    problems = test_floor.evaluate(_counts(skipped=2), FLOOR)
    assert len(problems) == 1
    assert "the allowance is 1" in problems[0]


def test_allows_skips_up_to_the_allowance() -> None:
    """The GEM end-to-end test is gated on xlsx files no runner has."""
    assert test_floor.evaluate(_counts(skipped=1), FLOOR) == []


@pytest.mark.parametrize("field", ["tests", "max_skipped"])
def test_refuses_an_unmeasured_floor_instead_of_passing_by_default(field: str) -> None:
    """`None` must read as "nobody measured this", never as "no limit, so fine"."""
    floor = dict(FLOOR)
    floor[field] = None
    problems = test_floor.evaluate(_counts(), floor)
    assert len(problems) == 1
    assert "scripts/test_floor.py" in problems[0]


def test_the_recorded_floor_is_measured_not_left_unset() -> None:
    """The shipped FLOOR must carry real numbers.

    `None` is the bootstrap state: the gate reports what it saw and fails, so
    the first CI run measures the floor and the second commit records it. This
    test is what stops that bootstrap state from becoming permanent.
    """
    assert test_floor.FLOOR["tests"] is not None, "run CI once, then record the count"
    assert test_floor.FLOOR["max_skipped"] is not None
    assert test_floor.FLOOR["tests"] > 0
