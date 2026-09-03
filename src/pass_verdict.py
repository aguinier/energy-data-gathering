"""Was this update pass healthy, empty, or collapsed? (ABL-61)

``scripts/update.py`` used to exit 0 on a total upstream outage: per-country
failures are caught and counted inside the pipeline, so 39/39 failing looked
exactly like 1/39 to anything supervising the container. This module is the
judgement, kept pure and separate from the pass that produces the numbers so it
can be tested against the two shapes actually observed on prod:

* **Empty** — 2026-08-06 13:30 UTC: 484 HTTP 503s across every document type,
  **0 of 30 countries stored**, and the process exited 0. Transient; it was gone
  by the evening pass. This is the case a whole-pass retry can fix.
* **Collapsed** — ABL-630, 2026-08-29 to 09-02: stored records fell from
  ~520,000/day to 9,408/day (**-98%**) for four days and self-resolved with no
  intervention. Every pass stored *something*, so no "stored nothing" rule sees
  it; only a comparison against what the same pass did recently does.

Deliberately **not** symmetric in what it asks for afterwards: an empty pass is
cheap to re-run and usually transient, so it earns a retry, while a collapsed
pass ran for its full 17-55 minutes and, in the one instance on record, stayed
collapsed for four days — re-running it would triple the request load on an
upstream that is already degraded. Collapse alarms; it does not retry.

Nothing here reads a threshold from a config module: every number arrives as an
argument, so a test states the numbers it is asserting about.
"""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Optional, Sequence

#: A pass that stored a normal amount of data.
VERDICT_OK = "ok"
#: A pass that stored nothing at all, across every country and every type.
VERDICT_EMPTY = "empty"
#: A pass that stored an order of magnitude less than its own recent baseline.
VERDICT_COLLAPSED = "collapsed"

#: Exit codes. Distinct per verdict so a supervisor can tell "upstream is gone"
#: from "the pass crashed" without parsing the log.
EXIT_OK = 0
#: Reserved for the pre-existing failures: bad config, or `pipeline.update()` raised.
EXIT_ERROR = 1
EXIT_EMPTY_PASS = 2
EXIT_VOLUME_COLLAPSE = 3

_EXIT_CODES = {
    VERDICT_OK: EXIT_OK,
    VERDICT_EMPTY: EXIT_EMPTY_PASS,
    VERDICT_COLLAPSED: EXIT_VOLUME_COLLAPSE,
}


@dataclass(frozen=True)
class PassVerdict:
    """What a finished pass amounts to, and what the caller should do about it."""

    verdict: str
    reason: str
    #: Median stored-record count over the recent healthy passes, or ``None``
    #: when there was not enough history to judge volume.
    baseline: Optional[float] = None
    #: The count below which the pass counts as collapsed, or ``None`` as above.
    threshold: Optional[float] = None

    @property
    def exit_code(self) -> int:
        return _EXIT_CODES[self.verdict]

    @property
    def is_ok(self) -> bool:
        return self.verdict == VERDICT_OK

    @property
    def should_retry(self) -> bool:
        """Only an empty pass. See the module docstring for why not collapse."""
        return self.verdict == VERDICT_EMPTY


def classify_pass(
    *,
    stored_records: int,
    countries_processed: int,
    baseline_totals: Sequence[int] = (),
    collapse_fraction: float,
    min_baseline_passes: int,
    min_countries_for_outage: int = 2,
) -> PassVerdict:
    """Judge one finished pass.

    Args:
        stored_records: Rows the pass wrote (inserted + replaced), all countries
            and all data types.
        countries_processed: How many countries the pass actually walked.
        baseline_totals: ``stored_records`` of the most recent passes that were
            themselves judged healthy, newest first. Empty disables the volume
            check.
        collapse_fraction: Fraction of the baseline below which the pass counts
            as collapsed, e.g. ``0.25`` for "stored under a quarter of normal".
        min_baseline_passes: Refuse to judge volume on fewer than this many
            historical passes. A cold start must not alarm.
        min_countries_for_outage: A pass narrower than this cannot be evidence of
            an upstream outage — a single-country run of a zone that publishes
            nothing legitimately stores nothing.

    Returns:
        The verdict. Never raises; a supervision signal that can itself fail is
        not a supervision signal.
    """
    if countries_processed <= 0:
        return PassVerdict(
            VERDICT_OK,
            "no countries selected — nothing to judge",
        )

    if countries_processed < min_countries_for_outage:
        return PassVerdict(
            VERDICT_OK,
            f"single-country pass ({countries_processed} country); "
            f"stored {stored_records} records, not judged as an outage",
        )

    if stored_records <= 0:
        return PassVerdict(
            VERDICT_EMPTY,
            f"stored NOTHING across all {countries_processed} countries — "
            "upstream outage or a broken write path",
        )

    if len(baseline_totals) < min_baseline_passes:
        return PassVerdict(
            VERDICT_OK,
            f"stored {stored_records} records; volume not judged "
            f"({len(baseline_totals)} of {min_baseline_passes} baseline passes on record)",
        )

    baseline = float(median(baseline_totals))
    threshold = baseline * collapse_fraction

    if stored_records < threshold:
        return PassVerdict(
            VERDICT_COLLAPSED,
            f"stored {stored_records} records, under {collapse_fraction:.0%} of the "
            f"{baseline:.0f}-record median of the last {len(baseline_totals)} healthy passes "
            f"(threshold {threshold:.0f})",
            baseline=baseline,
            threshold=threshold,
        )

    return PassVerdict(
        VERDICT_OK,
        f"stored {stored_records} records against a {baseline:.0f}-record baseline",
        baseline=baseline,
        threshold=threshold,
    )
