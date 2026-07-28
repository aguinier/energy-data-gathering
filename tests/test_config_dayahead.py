"""Day-ahead data types must be flagged as such, or the pipeline never asks for D+1.

Regression test for the 2026-07-28 report "France's net position for tomorrow
shows nothing".

`net_position` is an A25 day-ahead document and `fetch_net_position` has always
called the client with `dayahead=True`, but the config block was missing
`is_dayahead`. `pipeline.update_recent()` reads that flag and nothing else:

    if include_dayahead and config.is_dayahead_data_type(data_type):
        fetch_end = end_dayahead      # end of tomorrow, 23:59:59 UTC
    else:
        fetch_end = end               # now

So the window was capped at the current hour, the D+1 market-coupling result was
never requested, and the dashboard had a permanent hole where tomorrow should be.
A comment in the config block already said "DAY-AHEAD, not realized" — prose did
not prevent the omission, so this asserts it instead.
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

# config.py calls load_dotenv() at import time; stub it so this test needs no
# environment and no .env file.
if "dotenv" not in sys.modules:  # pragma: no cover - trivial shim
    _dotenv = types.ModuleType("dotenv")
    _dotenv.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = _dotenv

import config  # noqa: E402


# Every data type whose ENTSO-E document is published ahead of delivery. If you
# add one, add it here too — that is the point of the test.
EXPECTED_DAYAHEAD_TYPES = {
    "price",
    "load_forecast_day_ahead",
    "wind_solar_forecast",
    "net_position",
}


@pytest.mark.parametrize("data_type", sorted(EXPECTED_DAYAHEAD_TYPES))
def test_dayahead_types_are_flagged(data_type: str) -> None:
    assert config.is_dayahead_data_type(data_type), (
        f"{data_type!r} is published ahead of delivery but is not flagged "
        "is_dayahead, so pipeline.update_recent() caps its fetch window at now "
        "and the future portion is never requested."
    )


def test_dayahead_set_is_exactly_as_expected() -> None:
    """Catches both a dropped flag and one added without updating this list."""
    assert set(config.get_dayahead_data_types()) == EXPECTED_DAYAHEAD_TYPES


def test_net_position_is_a_dayahead_document() -> None:
    """The flag must agree with what the document actually is.

    A25 is the day-ahead net position; serving it with a now-capped window is
    the specific bug this file guards.
    """
    cfg = config.ENTSOE_API_CONFIG["net_position"]
    assert cfg["document_type"] == "A25"
    assert cfg.get("is_dayahead") is True


def test_realized_types_are_not_flagged() -> None:
    """Realized series have nothing to fetch past now; flagging them would just
    add empty requests to every cron run."""
    for data_type in ("load", "renewable", "crossborder_flows"):
        assert not config.is_dayahead_data_type(data_type), (
            f"{data_type!r} is a realized series and should not request D+1"
        )
