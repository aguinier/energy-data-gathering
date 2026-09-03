"""Ingest logging: a failed pass must say so, and must say why (ABL-633).

Two defects, both in `src/db.py`'s `log_ingestion_complete`, both silent.

**The status never reflected failure.** Status was
`"failed" if error_message else "completed"`, and not one caller that reports a
failure count also passes an error message -- `src/pipeline.py`,
`src/fetch_*.py` and `scripts/update_weather.py` all pass
`records_failed=failed` alone. So `records_failed` had no effect on `status`
whatsoever. Across the 2026-08-30..09-02 prod degradation every pass wrote
`completed` while `records_failed` reached 770/day and `records_inserted` fell
98% (1,280 passes -> 9,408 rows on 2026-09-01, against 562,480 on 2026-08-28).
Nothing downstream could key an alert on it.

**The error message was erased by the pass that reported the totals.** Each
fetcher catches its exception and writes it against `log_id` -- but only when a
`log_id` was handed down. Then its caller calls `log_ingestion_complete` a
second time with the run totals and `error_message=None`, and the old
unconditional `SET error_message = ?` wrote that `NULL` straight over the
reason. On the scheduled pipeline path it was worse still: `_fetch_data_chunk`
never passed `log_id` at all, so the reason was never written even once. Those
two paths cover every producer, which is why the table contained **zero**
non-empty `error_message` values -- verified on the local replica, 4 of 4
`records_failed = 1` rows carry `status = 'completed'` and a NULL reason.

`records_failed` is a run-level flag, not a row count: every fetcher's error
path returns `(0, 0, 1)`. So `partial_failure` is unreachable from today's
callers and the tests below pin it at the helper level only -- it exists so the
alertable predicate stays `status != 'completed'` if a fetcher ever learns to
report per-row failures.
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pytest
import pytz

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: E402
from src import db  # noqa: E402


# The live DDL, copied from the shared database. `status` is plain TEXT with no
# CHECK constraint, which is what makes widening the vocabulary a write-side
# change and not a migration against a database this repo does not own.
INGESTION_LOG_DDL = """
CREATE TABLE data_ingestion_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_type TEXT NOT NULL,
    country_code TEXT,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status TEXT NOT NULL,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


# ============================================================================
# resolve_ingestion_status: the pure mapping
# ============================================================================

def test_clean_pass_is_completed():
    assert db.resolve_ingestion_status(
        records_inserted=24, records_updated=0, records_failed=0
    ) == "completed"


def test_pass_that_stored_nothing_and_failed_nothing_is_still_completed():
    # ENTSO-E publishing nothing yet is not a failure. The fetchers return
    # (0, 0, 0) for both the empty-frame and ENTSOENoDataError paths, and the
    # dashboard reads that as "checked, no data" -- it must not become an alert.
    assert db.resolve_ingestion_status() == "completed"


def test_failure_count_alone_drives_the_status():
    # The whole defect: no error_message, yet the pass failed. This returned
    # "completed" before ABL-633.
    assert db.resolve_ingestion_status(records_failed=1) == "failed"


def test_error_message_alone_still_drives_the_status():
    # Preserved from the old behaviour: a caller that reports only a reason.
    assert db.resolve_ingestion_status(error_message="HTTP 401") == "failed"


def test_rows_stored_alongside_a_failure_is_partial():
    assert db.resolve_ingestion_status(
        records_inserted=20, records_failed=4
    ) == "partial_failure"


def test_updates_alone_count_as_stored_rows():
    # An upsert that only revised existing rows delivered data just as much as
    # one that inserted them; it must not be graded as a total failure.
    assert db.resolve_ingestion_status(
        records_updated=20, records_failed=4
    ) == "partial_failure"


def test_none_counts_are_treated_as_zero():
    # sqlite columns are nullable and callers pass values straight through.
    assert db.resolve_ingestion_status(None, None, None, None) == "completed"


def test_every_failure_status_is_alertable_by_one_predicate():
    # What a monitor keys on. If this drifts, an alert silently stops firing.
    assert db.INGESTION_FAILURE_STATUSES == {"partial_failure", "failed"}
    assert db.INGESTION_STATUS_COMPLETED not in db.INGESTION_FAILURE_STATUSES
    for status in db.INGESTION_FAILURE_STATUSES:
        assert status != db.INGESTION_STATUS_COMPLETED


# ============================================================================
# log_ingestion_complete: what actually lands in the row
# ============================================================================

@pytest.fixture
def log_db(tmp_path, monkeypatch):
    """A real sqlite file carrying the live data_ingestion_log DDL, wired in as
    the module's database so log_ingestion_start/_complete run unmodified."""
    path = tmp_path / "ingest_log.db"
    conn = sqlite3.connect(path)
    conn.execute(INGESTION_LOG_DDL)
    conn.commit()
    conn.close()

    monkeypatch.setattr(config, "DATABASE_PATH", path)
    return path


def _row(path, log_id):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            "SELECT * FROM data_ingestion_log WHERE id = ?", (log_id,)
        ).fetchone()
    finally:
        conn.close()


def test_start_marks_the_pass_running(log_db):
    log_id = db.log_ingestion_start("load", "DE")
    row = _row(log_db, log_id)

    assert row["status"] == "running"
    assert row["end_time"] is None


def test_completion_without_failures_records_completed_and_no_error(log_db):
    log_id = db.log_ingestion_start("load", "DE")
    db.log_ingestion_complete(log_id, records_inserted=24, records_updated=0)

    row = _row(log_db, log_id)
    assert row["status"] == "completed"
    assert row["error_message"] is None
    assert row["end_time"] is not None


def test_failure_count_without_a_reason_is_not_recorded_as_completed(log_db):
    # The exact prod shape during 2026-08-30..09-02: pipeline.py reports the
    # totals and nothing else.
    log_id = db.log_ingestion_start("price", "FR")
    db.log_ingestion_complete(log_id, records_failed=1)

    row = _row(log_db, log_id)
    assert row["status"] == "failed"
    # No reason was available anywhere. Say that, rather than leaving a NULL a
    # reader cannot distinguish from "we never looked".
    assert row["error_message"] == "no error detail captured"


def test_summary_call_does_not_erase_the_reason_the_fetcher_recorded(log_db):
    # The two-call sequence every fetcher runs: the inner call records the
    # exception, the outer call records the totals. Before ABL-633 the outer
    # call's error_message=None overwrote the reason with NULL.
    log_id = db.log_ingestion_start("load", "DE")

    db.log_ingestion_complete(log_id, records_failed=1, error_message="HTTP 429 from ENTSO-E")
    db.log_ingestion_complete(log_id, records_inserted=0, records_updated=0, records_failed=1)

    row = _row(log_db, log_id)
    assert row["error_message"] == "HTTP 429 from ENTSO-E"
    assert row["status"] == "failed"
    assert row["records_failed"] == 1


def test_a_later_call_may_refine_the_reason(log_db):
    # COALESCE keeps a stored reason only when this call supplies none; a caller
    # with a better reason is still able to write it.
    log_id = db.log_ingestion_start("load", "DE")

    db.log_ingestion_complete(log_id, records_failed=1, error_message="first")
    db.log_ingestion_complete(log_id, records_failed=1, error_message="second")

    assert _row(log_db, log_id)["error_message"] == "second"


def test_the_no_detail_marker_never_displaces_a_real_reason(log_db):
    # The fallback is a last resort. If a reason is already on the row, the
    # summary call must not replace it with the marker.
    log_id = db.log_ingestion_start("net_position", "NL")

    db.log_ingestion_complete(log_id, records_failed=1, error_message="connection reset")
    db.log_ingestion_complete(log_id, records_failed=1)

    assert _row(log_db, log_id)["error_message"] == "connection reset"


def test_partial_failure_keeps_the_stored_row_count(log_db):
    log_id = db.log_ingestion_start("load", "DE")
    db.log_ingestion_complete(
        log_id, records_inserted=20, records_failed=4, error_message="4 rows rejected"
    )

    row = _row(log_db, log_id)
    assert row["status"] == "partial_failure"
    assert row["records_inserted"] == 20
    assert row["error_message"] == "4 rows rejected"


# ============================================================================
# The scheduled pipeline: log_id must reach the fetcher that catches the error
# ============================================================================

# (module attribute, fetcher function, extra positional args before log_id)
PIPELINE_FETCHERS = [
    ("load", "fetch_load", "fetch_load_data", ()),
    ("price", "fetch_price", "fetch_price_data", ()),
    ("renewable", "fetch_renewable", "fetch_renewable_data", ()),
    ("load_forecast_day_ahead", "fetch_load_forecast", "fetch_load_forecast_data", ("day_ahead",)),
    ("load_forecast_week_ahead", "fetch_load_forecast", "fetch_load_forecast_data", ("week_ahead",)),
    ("wind_solar_forecast", "fetch_wind_solar_forecast", "fetch_wind_solar_forecast_data", ()),
    ("crossborder_flows", "fetch_crossborder_flows", "fetch_crossborder_flows_data", ()),
    ("net_position", "fetch_net_position", "fetch_net_position_data", ()),
]


@pytest.mark.parametrize(
    "data_type,module_name,func_name,extra_args",
    PIPELINE_FETCHERS,
    ids=[f[0] for f in PIPELINE_FETCHERS],
)
def test_pipeline_hands_the_log_id_to_every_fetcher(
    data_type, module_name, func_name, extra_args, monkeypatch
):
    """`_fetch_data_chunk` is the scheduled path -- `scripts/update.py` ->
    `pipeline.update()` -- and it is the one that ran 1,280 times a day through
    the outage. It called the fetchers without `log_id`, so each fetcher's
    `if log_id:` branch never fired and the exception it caught was written
    nowhere. A fetcher missed here silently loses its reason again."""
    from src import pipeline

    seen = {}

    def recorder(*args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        return 0, 0, 1

    module = getattr(pipeline, module_name)
    monkeypatch.setattr(module, func_name, recorder)
    monkeypatch.setattr(pipeline.db, "log_ingestion_start", lambda *a, **k: 4242)
    monkeypatch.setattr(pipeline.db, "log_ingestion_complete", lambda *a, **k: None)

    # Bypass __init__: constructing ENTSOEPipeline builds a live ENTSOEClient.
    p = object.__new__(pipeline.ENTSOEPipeline)
    p.client = object()
    p.stats = {"total_records": 0}

    start = datetime(2026, 9, 1, tzinfo=pytz.UTC)
    end = datetime(2026, 9, 2, tzinfo=pytz.UTC)
    ok = p._fetch_data_chunk(data_type, "DE", start, end)

    assert ok is False, "a fetcher returning failed=1 is not a success"
    positional = seen["args"]
    assert positional[1:] == ("DE", start, end) + extra_args + (4242,), (
        f"{module_name}.{func_name} did not receive log_id as its last argument"
    )
