"""The `(inserted, updated, failed)` triple a fetcher returns — plus *why* it failed.

Every ``fetch_*_data`` function catches its own exceptions and reports the
failure as the third element of a plain tuple. The reason died there:
``pipeline._fetch_data_chunk`` only ever saw ``(0, 0, 1)``, so it wrote
``records_failed = 1`` with ``error_message = NULL`` and — because
``log_ingestion_complete`` derives the status from the message —
``status = 'completed'`` on a row that recorded a failure.

That is not a hypothetical. Over the ABL-630 window (2026-08-29 to 09-02,
measured on prod) **all 2,370 `data_ingestion_log` rows carrying
`records_failed > 0` had `error_message` NULL, and all 5,696 runs in the window
said `completed`** — a four-day, portfolio-wide, -98% collapse left behind a
failure *count* and nothing else, and the cause is still unknown because of it.

``FetchResult`` is a ``tuple`` subclass so that every existing
``inserted, updated, failed = fetch_x(...)`` unpack, every ``== (0, 0, 0)``
assertion and every ``sum()`` keeps working untouched; the reason rides along as
an attribute, read back with :func:`error_of` so a caller handed a plain tuple
by some other code path still works.

**Build the reason with `utils.format_error`, never with `str(exc)`.** An
ENTSO-E ``HTTPError`` stringifies to the full request URL, credential and all
(ABL-86) — and this is the change that makes ``error_message`` a *populated*
column for the first time (0 of 3,230 rows carry one today). ``format_error``
redacts, and ``db.log_ingestion_complete`` redacts again on the way in, so the
key cannot reach the database through a caller that forgets.
"""

from __future__ import annotations

from typing import Any, Optional


class FetchResult(tuple):
    """``(records_inserted, records_updated, records_failed)`` with a reason.

    ``error`` is ``None`` on success and on "upstream published nothing", which
    is not a failure. It is set only when ``records_failed > 0``.
    """

    # No `__slots__`: CPython refuses a nonempty one on a variable-length
    # immutable base ("nonempty __slots__ not supported for subtype of
    # 'tuple'"), so the attribute lives in a per-instance dict. That is 312
    # small dicts per pass, once each.

    def __new__(
        cls,
        inserted: int,
        updated: int,
        failed: int,
        error: Optional[str] = None,
    ) -> "FetchResult":
        self = super().__new__(cls, (inserted, updated, failed))
        self.error = error
        return self

    @classmethod
    def failure(cls, error: str) -> "FetchResult":
        """One failed target, with the reason attached."""
        return cls(0, 0, 1, error=error)

    @property
    def inserted(self) -> int:
        return self[0]

    @property
    def updated(self) -> int:
        return self[1]

    @property
    def failed(self) -> int:
        return self[2]

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"FetchResult({self[0]}, {self[1]}, {self[2]}, error={self.error!r})"


def error_of(result: Any) -> Optional[str]:
    """The failure reason carried by ``result``, or ``None``.

    Tolerates a plain tuple: a fetcher that has not been converted yet, or a
    test double, returns one and must not raise here.
    """
    return getattr(result, "error", None)
