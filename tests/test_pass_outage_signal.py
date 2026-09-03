"""A pass that stores nothing must say so — in the exit code, the log and the row.

ABL-61, promoted after ABL-630. Two real incidents define the shapes pinned here:

* **2026-08-06 13:30 UTC** — 484 HTTP 503s across every document type, **0 of 30
  countries stored**, and `scripts/update.py` exited **0**. `pipeline.update()`
  did not raise (every per-country failure is caught and counted inside it), and
  exiting non-zero was the only thing the script did with a failure.
* **2026-08-29 to 09-02** — stored records fell from ~520,000/day to 9,408/day
  (**-98%**) for four days and self-resolved. Every pass stored *something*, so
  "stored nothing" cannot see it. What it left behind: **2,370 rows with
  `records_failed > 0` and `error_message` NULL**, **5,696 runs all saying
  `status='completed'`**, and an empty `docker logs`. The cause is still unknown,
  which is the defect — not the outage.

So there are three separable properties, and this file pins each on its own:

1. a failure's *reason* reaches the database (`error_message`, `status`),
2. a pass that stored nothing exits non-zero and re-runs itself,
3. a pass that stored an order of magnitude less than normal exits non-zero and
   does *not* re-run itself.
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import pytz

sys.path.insert(0, str(Path(__file__).parent.parent))

import config  # noqa: E402
import utils  # noqa: E402
from src import db  # noqa: E402
from src import log_redaction  # noqa: E402
from src import pipeline as pipeline_mod  # noqa: E402
from src.fetch_result import FetchResult, error_of  # noqa: E402
from src.pass_verdict import (  # noqa: E402
    EXIT_EMPTY_PASS,
    EXIT_OK,
    EXIT_VOLUME_COLLAPSE,
    VERDICT_COLLAPSED,
    VERDICT_EMPTY,
    VERDICT_OK,
    classify_pass,
)
from scripts import update as update_script  # noqa: E402


# The table as `database_structure.md` documents it. This repo never creates it
# -- it predates the pipeline -- so a test that writes to it brings its own.
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

# Per-pass stored-record counts either side of the ABL-630 collapse, derived from
# the prod daily totals in the issue thread at four full passes a day.
HEALTHY_PASS = 130_000            # 2026-08-27/28: ~520-560k a day
MODERATE_PASS = 98_000            # 2026-08-29: 392k a day, 75% of normal
HALVED_PASS = 58_000              # 2026-08-30: 233k a day, 45% of normal
COLLAPSED_PASS = 2_352            # 2026-09-01: 9,408 a day, 1.8% of normal


@pytest.fixture
def scratch_log_db(tmp_path, monkeypatch):
    """A database holding nothing but `data_ingestion_log`."""
    path = tmp_path / "ingestion_log.db"
    conn = sqlite3.connect(path)
    conn.execute(INGESTION_LOG_DDL)
    conn.commit()
    conn.close()
    monkeypatch.setattr(config, "DATABASE_PATH", path)
    return path


def read_log_rows(path: Path) -> list:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute("SELECT * FROM data_ingestion_log ORDER BY id").fetchall()
    finally:
        conn.close()


# ============================================================================
# FetchResult -- the reason travels with the count
# ============================================================================


def test_fetch_result_unpacks_exactly_like_the_tuple_it_replaces():
    """Every existing caller does `a, b, c = fetch_x(...)`. None of them change."""
    inserted, updated, failed = FetchResult(12, 3, 0)
    assert (inserted, updated, failed) == (12, 3, 0)


def test_fetch_result_compares_equal_to_a_plain_tuple():
    """Existing assertions read `== (0, 0, 0)`; a new type must not break them."""
    assert FetchResult(0, 0, 0) == (0, 0, 0)
    assert FetchResult(0, 0, 1, error="boom") == (0, 0, 1)


def test_failure_is_one_failed_target_carrying_its_reason():
    result = FetchResult.failure("fetch_load_data(DE): HTTPError: 503")
    assert result == (0, 0, 1)
    assert error_of(result) == "fetch_load_data(DE): HTTPError: 503"


def test_error_of_tolerates_a_plain_tuple():
    """A fetcher this change has not converted, or a test double, returns one."""
    assert error_of((0, 0, 1)) is None


def test_a_successful_result_carries_no_reason():
    assert error_of(FetchResult(24, 0, 0)) is None


# ============================================================================
# classify_pass -- the verdict
# ============================================================================


def classify(**overrides):
    kwargs = dict(
        stored_records=HEALTHY_PASS,
        countries_processed=39,
        baseline_totals=[HEALTHY_PASS] * 12,
        collapse_fraction=0.25,
        min_baseline_passes=4,
    )
    kwargs.update(overrides)
    return classify_pass(**kwargs)


def test_a_normal_pass_is_ok_and_exits_zero():
    verdict = classify()
    assert (verdict.verdict, verdict.exit_code) == (VERDICT_OK, EXIT_OK)
    assert not verdict.should_retry


def test_a_pass_that_stored_nothing_is_empty_and_exits_two():
    """2026-08-06: 0 of 30 countries stored, and the process exited 0."""
    verdict = classify(stored_records=0, countries_processed=30)
    assert verdict.verdict == VERDICT_EMPTY
    assert verdict.exit_code == EXIT_EMPTY_PASS
    assert verdict.should_retry, "an empty pass is the case a whole-pass retry is for"


def test_an_empty_pass_is_judged_without_any_baseline():
    """The outage this catches can arrive on a container's first pass."""
    verdict = classify(stored_records=0, countries_processed=30, baseline_totals=[])
    assert verdict.verdict == VERDICT_EMPTY


def test_a_single_country_run_storing_nothing_is_not_an_outage():
    """`--countries IS` legitimately stores nothing; retrying it would sleep 20
    minutes to learn the same thing again."""
    verdict = classify(stored_records=0, countries_processed=1)
    assert verdict.verdict == VERDICT_OK
    assert not verdict.should_retry


def test_a_pass_over_no_countries_is_not_an_outage():
    """A typo'd `--countries` leaves nothing to fetch; that is a caller error,
    and it must not put the container into a retry loop."""
    verdict = classify(stored_records=0, countries_processed=0)
    assert verdict.verdict == VERDICT_OK


def test_the_abl630_collapse_is_caught():
    """The shape "stored something, but 2% of normal" — invisible to rule 1."""
    verdict = classify(stored_records=COLLAPSED_PASS)
    assert verdict.verdict == VERDICT_COLLAPSED
    assert verdict.exit_code == EXIT_VOLUME_COLLAPSE


def test_a_collapsed_pass_is_not_retried():
    """It ran its full 17-55 minutes and, on the one instance on record, stayed
    collapsed for four days. Re-running it triples load on a degraded upstream."""
    assert not classify(stored_records=COLLAPSED_PASS).should_retry


@pytest.mark.parametrize(
    "stored, label",
    [(MODERATE_PASS, "75% of normal"), (HALVED_PASS, "45% of normal")],
)
def test_a_merely_quiet_pass_does_not_alarm(stored, label):
    """Deliberate. An alarm that fires on a half-strength day gets muted, and
    then the 2% day is missed too."""
    assert classify(stored_records=stored).verdict == VERDICT_OK, label


def test_volume_is_not_judged_on_a_cold_start():
    """A freshly deployed container has no pass rows. Alarming on its first pass
    would teach everyone to ignore the alarm on day one."""
    verdict = classify(stored_records=COLLAPSED_PASS, baseline_totals=[HEALTHY_PASS] * 3)
    assert verdict.verdict == VERDICT_OK
    assert verdict.baseline is None


def test_one_bad_pass_in_the_baseline_does_not_move_the_median():
    """Median, not mean: a single collapsed pass that slipped into the history
    must not lower the bar for the next one."""
    baseline = [COLLAPSED_PASS] + [HEALTHY_PASS] * 11
    assert classify(stored_records=COLLAPSED_PASS, baseline_totals=baseline).verdict == (
        VERDICT_COLLAPSED
    )


def test_the_verdict_reason_names_the_numbers_it_judged_on():
    """The reason is what lands in `error_message` and in the log line; a bare
    "collapsed" would repeat the ABL-630 problem in a new column."""
    verdict = classify(stored_records=COLLAPSED_PASS)
    assert str(COLLAPSED_PASS) in verdict.reason
    assert f"{HEALTHY_PASS:.0f}" in verdict.reason


# ============================================================================
# db.log_ingestion_complete -- the reason reaches the row
# ============================================================================


def test_a_reason_is_persisted_and_flips_the_status(scratch_log_db):
    log_id = db.log_ingestion_start("load", "DE")
    db.log_ingestion_complete(log_id, records_failed=1, error_message="HTTPError: 503")

    row = read_log_rows(scratch_log_db)[0]
    assert row["error_message"] == "HTTPError: 503"
    assert row["status"] == "failed"


def test_a_healthy_completion_leaves_the_reason_null(scratch_log_db):
    log_id = db.log_ingestion_start("load", "DE")
    db.log_ingestion_complete(log_id, records_inserted=168)

    row = read_log_rows(scratch_log_db)[0]
    assert row["error_message"] is None
    assert row["status"] == "completed"


def test_the_credential_never_reaches_the_column(scratch_log_db):
    """This change makes `error_message` a populated column for the first time,
    and an ENTSO-E HTTPError stringifies to the request URL — securityToken and
    all (ABL-86). Redaction is here, not at the 17 call sites."""
    secret = "11111111-2222-3333-4444-555555555555"
    log_id = db.log_ingestion_start("price", "FR")
    db.log_ingestion_complete(
        log_id,
        records_failed=1,
        error_message=(
            "503 Server Error for url: https://web-api.tp.entsoe.eu/api"
            f"?documentType=A44&securityToken={secret}"
        ),
    )

    stored = read_log_rows(scratch_log_db)[0]["error_message"]
    assert secret not in stored
    assert "securityToken=" in stored, "the rest of the URL is the diagnosis; keep it"
    assert "A44" in stored


def test_a_registered_key_is_scrubbed_even_without_the_parameter_name(scratch_log_db):
    secret = "abcdef01-2345-6789-abcd-ef0123456789"
    log_redaction.register_secret_value(secret)
    log_id = db.log_ingestion_start("load", "DE")
    db.log_ingestion_complete(
        log_id, records_failed=1, error_message=f"auth rejected for {secret}"
    )
    assert secret not in read_log_rows(scratch_log_db)[0]["error_message"]


def test_a_huge_upstream_body_is_truncated(scratch_log_db):
    log_id = db.log_ingestion_start("load", "DE")
    db.log_ingestion_complete(log_id, records_failed=1, error_message="x" * 5000)

    stored = read_log_rows(scratch_log_db)[0]["error_message"]
    assert len(stored) == db.MAX_ERROR_MESSAGE_CHARS
    assert stored.endswith("...")


def test_a_blank_reason_is_no_reason(scratch_log_db):
    """An empty string would set status='failed' while telling the next reader
    exactly as much as the NULL it replaced."""
    log_id = db.log_ingestion_start("load", "DE")
    db.log_ingestion_complete(log_id, records_failed=1, error_message="   \n  ")

    row = read_log_rows(scratch_log_db)[0]
    assert row["error_message"] is None
    assert row["status"] == "completed"


# ============================================================================
# db.recent_pass_totals -- the baseline
# ============================================================================


def insert_pass(path: Path, records: int, error: str = None, finished: bool = True):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO data_ingestion_log "
        "(pipeline_type, country_code, start_time, end_time, status, "
        " records_inserted, error_message) VALUES (?, NULL, ?, ?, ?, ?, ?)",
        (
            db.PASS_PIPELINE_TYPE,
            "2026-09-01T00:30:00+00:00",
            "2026-09-01T01:10:00+00:00" if finished else None,
            "failed" if error else "completed",
            records,
            error,
        ),
    )
    conn.commit()
    conn.close()


def test_the_baseline_is_the_recent_passes_newest_first(scratch_log_db):
    for records in (100, 200, 300):
        insert_pass(scratch_log_db, records)
    assert db.recent_pass_totals(10) == [300, 200, 100]
    assert db.recent_pass_totals(2) == [300, 200]


def test_an_unhealthy_pass_is_not_part_of_the_baseline(scratch_log_db):
    """Otherwise a multi-day collapse teaches the baseline that collapse is
    normal and the alarm silences itself on day two — the exact shape ABL-630
    had."""
    insert_pass(scratch_log_db, HEALTHY_PASS)
    insert_pass(scratch_log_db, COLLAPSED_PASS, error="collapsed: stored 2,352 records")
    assert db.recent_pass_totals(10) == [HEALTHY_PASS]


def test_the_in_flight_pass_is_not_part_of_its_own_baseline(scratch_log_db):
    insert_pass(scratch_log_db, HEALTHY_PASS)
    insert_pass(scratch_log_db, 0, finished=False)
    assert db.recent_pass_totals(10) == [HEALTHY_PASS]


def test_per_country_rows_are_not_mistaken_for_passes(scratch_log_db):
    log_id = db.log_ingestion_start("load", "DE")
    db.log_ingestion_complete(log_id, records_inserted=168)
    assert db.recent_pass_totals(10) == []


# ============================================================================
# pipeline._fetch_data_chunk -- where the reason used to die
# ============================================================================


@pytest.fixture
def stub_pipeline(monkeypatch):
    monkeypatch.setattr(pipeline_mod, "ENTSOEClient", MagicMock)
    return pipeline_mod.ENTSOEPipeline()


def run_load_chunk(pipeline, monkeypatch, result):
    monkeypatch.setattr(
        pipeline_mod.fetch_load, "fetch_load_data", MagicMock(return_value=result)
    )
    start = pytz.UTC.localize(datetime(2026, 9, 1))
    end = pytz.UTC.localize(datetime(2026, 9, 2))
    return pipeline._fetch_data_chunk("load", "DE", start, end)


def test_a_fetcher_failure_lands_in_the_row_with_its_reason(
    scratch_log_db, stub_pipeline, monkeypatch
):
    """The defect itself: this call site passes no `log_id`, so the fetcher's own
    logging branch never ran, and the reason was dropped on the floor."""
    reason = "fetch_load_data(DE): HTTPError: 503 Server Error"
    assert run_load_chunk(stub_pipeline, monkeypatch, FetchResult.failure(reason)) is False

    row = read_log_rows(scratch_log_db)[0]
    assert row["records_failed"] == 1
    assert row["error_message"] == reason
    assert row["status"] == "failed"


def test_a_failure_without_a_reason_still_gets_one(
    scratch_log_db, stub_pipeline, monkeypatch
):
    """`records_failed > 0` with `error_message` NULL is the row shape this issue
    exists to remove. No path may produce it — including a fetcher that has not
    been converted, or a future one written from the old template."""
    assert run_load_chunk(stub_pipeline, monkeypatch, (0, 0, 1)) is False

    row = read_log_rows(scratch_log_db)[0]
    assert row["records_failed"] == 1
    assert row["error_message"], "a failure row must never carry a NULL reason"
    assert "load" in row["error_message"] and "DE" in row["error_message"]


def test_a_successful_chunk_records_no_reason(scratch_log_db, stub_pipeline, monkeypatch):
    assert run_load_chunk(stub_pipeline, monkeypatch, FetchResult(168, 0, 0)) is True

    row = read_log_rows(scratch_log_db)[0]
    assert (row["records_inserted"], row["records_failed"]) == (168, 0)
    assert row["error_message"] is None
    assert row["status"] == "completed"


def test_upstream_publishing_nothing_is_not_a_failure(
    scratch_log_db, stub_pipeline, monkeypatch
):
    """A quiet zone must not be reported as an error, or the signal is noise."""
    assert run_load_chunk(stub_pipeline, monkeypatch, FetchResult(0, 0, 0)) is True
    assert read_log_rows(scratch_log_db)[0]["status"] == "completed"


def test_a_raising_fetcher_is_recorded_redacted(scratch_log_db, stub_pipeline, monkeypatch):
    """The outer safety net. Its message used to go in as a raw `str(e)`."""
    secret = "99999999-8888-7777-6666-555555555555"
    boom = RuntimeError(f"boom https://web-api.tp.entsoe.eu/api?securityToken={secret}")
    monkeypatch.setattr(
        pipeline_mod.fetch_load, "fetch_load_data", MagicMock(side_effect=boom)
    )
    start = pytz.UTC.localize(datetime(2026, 9, 1))
    end = pytz.UTC.localize(datetime(2026, 9, 2))

    assert stub_pipeline._fetch_data_chunk("load", "DE", start, end) is False

    row = read_log_rows(scratch_log_db)[0]
    assert row["status"] == "failed"
    assert secret not in row["error_message"]
    assert "RuntimeError" in row["error_message"]
    assert stub_pipeline.stats["failed_targets"] == 1


def test_an_unknown_data_type_is_counted_as_a_failed_target(scratch_log_db, stub_pipeline):
    start = pytz.UTC.localize(datetime(2026, 9, 1))
    end = pytz.UTC.localize(datetime(2026, 9, 2))
    assert stub_pipeline._fetch_data_chunk("not_a_type", "DE", start, end) is False
    assert stub_pipeline.stats["failed_targets"] == 1


# ============================================================================
# scripts/update.py -- exit codes and the whole-pass retry
# ============================================================================


def test_only_the_routine_pass_is_comparable_with_other_passes():
    """A `--types price --days 2` run stores a fraction of what a full pass
    stores. Judging it against the full-pass baseline would alarm daily at 11:15
    UTC; recording it as a baseline would drag the median down."""
    args = MagicMock(days=config.UPDATE_DAYS_BACK)
    assert update_script.is_full_pass(args, update_script.ALL_DATA_TYPES, None)
    assert not update_script.is_full_pass(args, ["price"], None)
    assert not update_script.is_full_pass(args, update_script.ALL_DATA_TYPES, ["DE"])

    narrow_window = MagicMock(days=2)
    assert not update_script.is_full_pass(
        narrow_window, update_script.ALL_DATA_TYPES, None
    )


@pytest.fixture
def run_main(monkeypatch, scratch_log_db):
    """Run `update.main()` over a stubbed pipeline, returning (exit code, sleeps)."""

    def run(pass_stats, argv=("update.py",), retry_delays=(300, 900)):
        monkeypatch.setattr(config, "validate_config", lambda: True)
        monkeypatch.setattr(config, "PASS_RETRY_DELAYS_SECONDS", list(retry_delays))
        monkeypatch.setattr(utils, "setup_logging", lambda **kw: MagicMock())
        monkeypatch.setattr(sys, "argv", list(argv))

        remaining = list(pass_stats)
        calls = []

        def fake_update(**kwargs):
            calls.append(kwargs)
            return remaining.pop(0) if len(remaining) > 1 else remaining[0]

        monkeypatch.setattr(update_script.pipeline, "update", fake_update)

        sleeps = []
        monkeypatch.setattr(update_script.time, "sleep", lambda s: sleeps.append(s))

        with pytest.raises(SystemExit) as exc:
            update_script.main()
        return exc.value.code, sleeps, calls

    return run


def stats(total_records, countries=39, failed_targets=0):
    return {
        "total_records": total_records,
        "total_countries": countries,
        "countries_with_records": countries if total_records else 0,
        "failed_targets": failed_targets,
        "successful_countries": countries,
        "failed_countries": 0,
        "by_data_type": {},
    }


def test_a_healthy_pass_still_exits_zero(run_main):
    code, sleeps, calls = run_main([stats(HEALTHY_PASS)])
    assert code == EXIT_OK
    assert sleeps == []
    assert len(calls) == 1


def test_a_total_outage_retries_the_whole_pass_then_exits_two(run_main):
    """The 2026-08-06 shape. Three per-request attempts one, two and four seconds
    apart all land inside a multi-minute outage; re-running the pass minutes
    later does not."""
    code, sleeps, calls = run_main([stats(0)])
    assert code == EXIT_EMPTY_PASS
    assert sleeps == [300, 900], "whole-pass retry, in minutes not seconds"
    assert len(calls) == 3, "the original pass plus one re-run per delay"


def test_a_recovered_retry_exits_zero(run_main):
    """A transient outage that clears must not page anyone."""
    code, sleeps, calls = run_main([stats(0), stats(HEALTHY_PASS)])
    assert code == EXIT_OK
    assert sleeps == [300], "stopped retrying as soon as the pass stored something"
    assert len(calls) == 2


def test_no_pass_retry_fails_fast(run_main):
    code, sleeps, calls = run_main([stats(0)], argv=("update.py", "--no-pass-retry"))
    assert code == EXIT_EMPTY_PASS
    assert sleeps == []
    assert len(calls) == 1


def test_the_pass_row_records_the_verdict(run_main, scratch_log_db):
    """`data_ingestion_log` is how ABL-630 was diagnosed at all, and the only
    supervision channel that survives log rotation."""
    run_main([stats(0)], argv=("update.py", "--no-pass-retry"))

    passes = [r for r in read_log_rows(scratch_log_db) if r["pipeline_type"] == db.PASS_PIPELINE_TYPE]
    assert len(passes) == 1
    assert passes[0]["status"] == "failed"
    assert "stored NOTHING" in passes[0]["error_message"]
    assert passes[0]["country_code"] is None


def test_a_healthy_pass_row_can_serve_as_a_baseline(run_main, scratch_log_db):
    run_main([stats(HEALTHY_PASS, failed_targets=3)])

    row = [r for r in read_log_rows(scratch_log_db) if r["pipeline_type"] == db.PASS_PIPELINE_TYPE][0]
    assert row["error_message"] is None
    assert row["records_inserted"] == HEALTHY_PASS
    assert row["records_failed"] == 3, "a few failed targets is a normal day"
    assert db.recent_pass_totals(10) == [HEALTHY_PASS]


def test_a_narrow_pass_writes_no_baseline_row(run_main, scratch_log_db):
    run_main([stats(500, countries=30)], argv=("update.py", "--types", "price", "--days", "2"))

    assert not [
        r for r in read_log_rows(scratch_log_db) if r["pipeline_type"] == db.PASS_PIPELINE_TYPE
    ]


def test_a_collapse_exits_three_without_retrying(run_main, scratch_log_db):
    for _ in range(config.PASS_MIN_BASELINE_PASSES):
        insert_pass(scratch_log_db, HEALTHY_PASS)

    code, sleeps, calls = run_main([stats(COLLAPSED_PASS)])
    assert code == EXIT_VOLUME_COLLAPSE
    assert sleeps == []
    assert len(calls) == 1


def test_a_crashing_pipeline_still_exits_one(scratch_log_db, monkeypatch):
    """The one failure mode the script always handled. It must not regress into
    an outage code."""
    monkeypatch.setattr(config, "validate_config", lambda: True)
    monkeypatch.setattr(update_script.utils, "setup_logging", lambda **kw: MagicMock())
    monkeypatch.setattr(sys, "argv", ["update.py"])

    def boom(**kwargs):
        raise RuntimeError("database is locked")

    monkeypatch.setattr(update_script.pipeline, "update", boom)

    with pytest.raises(SystemExit) as exc:
        update_script.main()
    assert exc.value.code == 1


def test_the_baseline_read_cannot_take_the_pass_down(monkeypatch):
    """Supervision that can break ingest is worse than no supervision. The
    replica is locked to all readers twice a day while `able-db-sync` runs."""
    logger = MagicMock()

    def locked(_limit):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(db, "recent_pass_totals", locked)
    assert update_script.read_baseline(logger, full_pass=True) == []
    assert logger.warning.called
