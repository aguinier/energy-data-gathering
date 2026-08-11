"""A late-publisher catch-up pass must actually be scheduled, and at low frequency.

ABL-85: the update cron only ever refetches a trailing 7-day window, so a zone
that goes dark longer than that and later republishes (AL, CY) is never
re-requested once the outage slides out of view. `scripts/catchup.py` is the
fix; this pins that it is wired into the image's crontab, and that it runs
at the low frequency the design relies on -- a healthy zone costs it one
bounded, indexed SELECT and zero ENTSO-E requests, which is exactly what makes
it safe to leave running indefinitely rather than a standing cost like
widening the whole fleet's window would be.
"""
from __future__ import annotations

import re
from pathlib import Path

CRONTAB = Path(__file__).parent.parent / "docker" / "crontab"


def _catchup_jobs() -> list[str]:
    """Every raw cron line that runs scripts/catchup.py."""
    return [
        line.strip()
        for line in CRONTAB.read_text(encoding="utf-8").splitlines()
        if "scripts/catchup.py" in line and not line.strip().startswith("#")
    ]


def test_crontab_exists() -> None:
    assert CRONTAB.is_file(), f"{CRONTAB} is the schedule the image installs"


def test_a_catchup_job_is_scheduled() -> None:
    jobs = _catchup_jobs()
    assert jobs, "no scripts/catchup.py job is scheduled at all"


def test_exactly_one_catchup_job() -> None:
    # Two schedules would double the (small, but nonzero) ENTSO-E request
    # volume for no benefit -- the recoverable window stays open for weeks,
    # so there is no freshness reason to run this more than once.
    assert len(_catchup_jobs()) == 1


def test_catchup_runs_weekly_not_daily_or_hourly() -> None:
    """The whole design relies on this being a low-frequency job.

    A standard 5-field cron line runs daily unless the day-of-week (field 5)
    or day-of-month (field 3) is restricted. Weekly means field 5 is a
    specific day (0-6) and field 3 is unrestricted ('*').
    """
    line = _catchup_jobs()[0]
    minute, hour, dom, month, dow, command = line.split(None, 5)

    assert dom == "*", f"expected day-of-month unrestricted, got {dom!r} in: {line}"
    assert month == "*", f"expected every month, got {month!r} in: {line}"
    assert re.fullmatch(r"[0-6]", dow), (
        f"expected a single specific weekday (0-6) so this runs once a week, "
        f"got dow={dow!r} in: {line}"
    )
    assert re.fullmatch(r"\d+", minute) and re.fullmatch(r"\d+", hour), (
        f"expected a fixed daily time, not a step/range that would fire more "
        f"than once a day, got {minute!r} {hour!r} in: {line}"
    )


def test_catchup_does_not_collide_with_another_scheduled_job() -> None:
    """It should cost nothing extra to run -- including not contending for the
    same minute as a full update, weather pull, or day-ahead price pass."""
    lines = [
        line.strip()
        for line in CRONTAB.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    catchup_line = _catchup_jobs()[0]
    catchup_minute, catchup_hour = catchup_line.split(None, 2)[:2]

    for line in lines:
        if line == catchup_line:
            continue
        minute, hour = line.split(None, 2)[:2]
        if minute == catchup_minute and hour == catchup_hour:
            raise AssertionError(f"catch-up job collides with another job at {hour}:{minute} UTC: {line}")
