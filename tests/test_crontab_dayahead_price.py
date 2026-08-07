"""Tomorrow's day-ahead price must be fetchable the same afternoon it is published.

ABL-54 (parent ABL-51, board-reported): "at 18:00 CEST the dashboard should show
tomorrow's day-ahead price but shows nothing."

The fetch *window* turned out to be innocent — `price` has carried
`is_dayahead: True` since the initial commit (see test_config_dayahead.py), and
prod's request on 2026-08-06 was literally
`documentType=A44&...&periodEnd=202608080000`, the end of D+1, on every pass.
The *schedule* was the exposure. SDAC publishes the whole next market day at
~12:45 Brussels local, and the first pass that could see it was the 13:30 UTC
full update — the only one before evening. On 2026-08-06 that pass met an
ENTSO-E outage (484 HTTP 503s, 0 of 30 countries stored) and tomorrow's price
did not exist for a user until 18:35 UTC.

So the property is not "a cron line exists" but "a price-capable pass runs
after publication and before the 13:30 full update, under BOTH DST regimes" —
publication is 10:45 UTC in CEST and 11:45 UTC in CET, and a single fixed-UTC
line cannot be prompt for one without being early for the other.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


CRONTAB = Path(__file__).parent.parent / "docker" / "crontab"

# UTC instant of the ~12:45 Brussels market-coupling publication, per regime.
PUBLICATION_UTC_MINUTES = {
    "CEST (summer, UTC+2)": 10 * 60 + 45,
    "CET  (winter, UTC+1)": 11 * 60 + 45,
}
# The full update that used to be the first and only chance.
FULL_AFTERNOON_PASS_MINUTES = 13 * 60 + 30


def _update_jobs() -> list[tuple[int, str]]:
    """Every `scripts/update.py` cron line, as (minutes past midnight UTC, command).

    Only the fixed-time daily form (`M H * * *`) is recognised; nothing in this
    file schedules `update.py` any other way, and a silently unparsed line would
    be worse than a failure here.
    """
    jobs: list[tuple[int, str]] = []
    for raw in CRONTAB.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "scripts/update.py" not in line:
            continue
        m = re.match(r"^(\d+)\s+(\d+)\s+\*\s+\*\s+\*\s+(.*)$", line)
        assert m, f"unrecognised schedule for an update.py job: {line!r}"
        minute, hour, command = int(m.group(1)), int(m.group(2)), m.group(3)
        jobs.append((hour * 60 + minute, command))
    return jobs


def _fetches_price(command: str) -> bool:
    """Does this invocation fetch price?

    `--types` absent means the script's default list, which includes price.
    """
    m = re.search(r"--types\s+(\S+)", command)
    if not m:
        return True
    return "price" in {t.strip() for t in m.group(1).split(",")}


def test_crontab_exists_and_schedules_updates() -> None:
    assert CRONTAB.is_file(), f"{CRONTAB} is the schedule the image installs"
    assert _update_jobs(), "no scripts/update.py job is scheduled at all"


@pytest.mark.parametrize("regime, published_at", sorted(PUBLICATION_UTC_MINUTES.items()))
def test_a_price_pass_runs_between_publication_and_the_afternoon_full_update(
    regime: str, published_at: int
) -> None:
    """Under each DST regime, tomorrow's price is fetched before 13:30 UTC."""
    candidates = [
        at
        for at, command in _update_jobs()
        if _fetches_price(command) and published_at <= at < FULL_AFTERNOON_PASS_MINUTES
    ]
    assert candidates, (
        f"under {regime} the day-ahead auction publishes at "
        f"{published_at // 60:02d}:{published_at % 60:02d} UTC and no price-fetching "
        "pass runs before the 13:30 UTC full update, so tomorrow's price waits for "
        "it — and a single upstream outage there (2026-08-06: 484 HTTP 503s) leaves "
        "the dashboard with no price for tomorrow until the 18:30 pass"
    )


def test_two_independent_attempts_before_the_afternoon_full_update() -> None:
    """One extra pass would still be a single point of failure.

    The 2026-08-06 incident was a whole-pass upstream outage, not a per-country
    error, so redundancy is the property that matters — not merely earliness.
    """
    before_full = [
        at
        for at, command in _update_jobs()
        if _fetches_price(command)
        and max(PUBLICATION_UTC_MINUTES.values()) - 60 <= at < FULL_AFTERNOON_PASS_MINUTES
    ]
    assert len(before_full) >= 2, (
        "expected at least two price passes in the run-up to 13:30 UTC, found "
        f"{len(before_full)}"
    )


def test_the_four_full_passes_are_still_scheduled() -> None:
    """The extra passes are additive. Dropping a full pass to pay for one would
    trade tomorrow's price against every other series' freshness."""
    full = {at for at, command in _update_jobs() if "--types" not in command}
    assert full == {0 * 60 + 30, 6 * 60 + 30, 13 * 60 + 30, 18 * 60 + 30}


def test_price_only_passes_stay_cheap() -> None:
    """A price-only pass is ~90s (A44 for ~30 countries at ~1.8s each, measured
    on prod 2026-08-05). That is what makes running it twice uncontroversial; a
    pass that quietly grew to the full type list would not be."""
    for at, command in _update_jobs():
        if at >= FULL_AFTERNOON_PASS_MINUTES or "--types" not in command:
            continue
        types = re.search(r"--types\s+(\S+)", command).group(1)
        assert types == "price", (
            f"the {at // 60:02d}:{at % 60:02d} UTC pass fetches {types!r}; if that is "
            "deliberate, update this test and the cost note in docker/crontab"
        )
