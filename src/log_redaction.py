"""
Keep the API credential out of the logs.

`requests` builds its `HTTPError` message out of the **full** request URL, and
entsoe-py puts our credential in that URL as `securityToken=`. So every failed
ENTSO-E request hands us an exception whose `str()` contains the key, and each
of the ~128 `logger.error(f"...: {e}")` sites in this repo writes it verbatim
into `logs/pipeline.log` and -- via cron's `2>&1` -- `logs/cron_update.log`.
Both files are owned by root, rotate slowly, and exist to be tailed and pasted
into diagnostics, so the credential sat in cleartext at rest and travelled
into every shared log excerpt. Found while diagnosing ABL-84; filed as ABL-86.

**The fix is central on purpose.** The string we must not print is one we never
construct -- it is built inside `requests` from a URL assembled inside
entsoe-py -- so a rule applied at the call site would have to be remembered at
all 128 of them, and at the 129th. Redacting here means a new `logger.error`
is safe without its author knowing this file exists.

Two independent rules, because neither one alone closes the hole:

- `redact_secrets` rewrites `<name>=<value>` (and the quoted mapping form
  `'<name>': '<value>'`) for a known-secret parameter *name*. It needs no
  knowledge of the credential, so it works in a test, on a machine with no
  `.env`, and against a key rotated five minutes ago.
- `register_secret_value` scrubs a literal *value* wherever it appears, in any
  shape at all. That covers what a name list cannot anticipate: a params dict
  repr (entsoe-py logs one at DEBUG), a bare key echoed into a message, a
  header dump. `ENTSOEClient.__init__` registers the key it was handed.

`SecretRedactingFilter` applies both. It is installed on the **handlers** of
the pipeline and root loggers rather than on a logger, because a handler filter
sees every record that reaches it -- ours, entsoe-py's and urllib3's alike --
while a logger filter sees only records logged directly through that logger.

`redact_exception` covers the one path logging cannot reach: an *uncaught*
exception is printed to stderr by the interpreter itself, and cron redirects
stderr into the same file. It rewrites the exception's `args` in place, which
is what `str(e)` -- and therefore the traceback -- reads.

What this deliberately does **not** do: it does not scrub a `print()` that
bypasses logging, and it does not touch the log files already written on prod.
This is the stop-the-bleeding control (ABL-86 step 1). Scrubbing the existing
prod logs and rotating the key are steps 2 and 3, escalated to the Board.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional, Set

REDACTED = "<redacted>"

PIPELINE_LOGGER_NAME = "entsoe_pipeline"

#: Parameter names whose value is a credential. Lower-case; matching is
#: case-insensitive. `securitytoken` is ENTSO-E's; `apikey` is Open-Meteo's
#: (`fetch_weather_observation.py`); the rest are here so that adding a new
#: upstream does not require rediscovering this defect. Extend this set rather
#: than adding a redaction call at a new call site.
SECRET_QUERY_PARAMS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "auth_token",
        "client_secret",
        "passwd",
        "password",
        "secret",
        "securitytoken",
        "token",
    }
)

#: A registered value shorter than this is refused. Redacting a short or common
#: string would blank unrelated text everywhere it happened to occur, which
#: turns a log into a worse lie than the one being fixed. Real credentials here
#: are UUIDs (36 chars).
MIN_REGISTERED_SECRET_LENGTH = 12

_NAMES = "|".join(sorted(SECRET_QUERY_PARAMS))

# `securityToken=<value>` in a query string. The value runs to the next `&`,
# whitespace, quote or bracket -- ENTSO-E error text embeds the URL in a
# sentence, so end-of-string and a trailing space are both real terminators.
_QUERY_PARAM_RE = re.compile(rf"\b({_NAMES})(=)([^&\s\"'<>\]\)]+)", re.IGNORECASE)

# `'securityToken': 'value'` -- the repr of a params dict, which is what
# entsoe-py logs at DEBUG level before it makes the request. Key and value
# quote characters are captured separately because a repr may mix them.
_MAPPING_RE = re.compile(
    rf"(['\"])({_NAMES})\1(\s*:\s*)(['\"])([^'\"]*)\4", re.IGNORECASE
)

_registered_secret_values: Set[str] = set()

_EXC_FORMATTER = logging.Formatter()


def register_secret_value(value: Any) -> bool:
    """
    Register a literal credential to be scrubbed wherever it appears.

    Complements the parameter-name rules: those know the *shape* a credential
    is written in, this knows the credential itself, and each catches what the
    other misses.

    Returns True if the value was registered. A non-string, or one shorter than
    `MIN_REGISTERED_SECRET_LENGTH`, is refused rather than accepted quietly --
    see that constant for why.
    """
    if not isinstance(value, str):
        return False
    value = value.strip()
    if len(value) < MIN_REGISTERED_SECRET_LENGTH:
        return False
    _registered_secret_values.add(value)
    return True


def registered_secret_count() -> int:
    """How many literal values are currently registered. For tests and health checks."""
    return len(_registered_secret_values)


def redact_secrets(text: Any) -> Any:
    """
    Return `text` with every credential we can recognise replaced by `REDACTED`.

    Non-strings are returned untouched, so this is safe to map over arbitrary
    logging args. Idempotent: `<redacted>` contains no secret to find.
    """
    if not isinstance(text, str) or not text:
        return text

    redacted = _QUERY_PARAM_RE.sub(rf"\1\2{REDACTED}", text)
    redacted = _MAPPING_RE.sub(rf"\1\2\1\3\4{REDACTED}\4", redacted)

    for value in _registered_secret_values:
        if value in redacted:
            redacted = redacted.replace(value, REDACTED)

    return redacted


def redact_exception(error: BaseException) -> BaseException:
    """
    Scrub credentials out of an exception's own message, in place, and return it.

    `str(e)` reads `e.args`, and so does the traceback the interpreter prints
    for an uncaught exception -- the one place a logging filter never sees.
    The chain reached through `__cause__` / `__context__` is scrubbed too,
    because `raise ENTSOEClientError(...) from e` prints the original
    `requests.HTTPError` underneath ours.

    Only string args that actually change are rewritten; a structured arg is
    left exactly as it was. This never raises: an exception type with read-only
    args must not turn a logged error into a crash.
    """
    _redact_exception_chain(error, set())
    return error


def _redact_exception_chain(error: Optional[BaseException], seen: Set[int]) -> None:
    if error is None or id(error) in seen:
        return
    seen.add(id(error))

    try:
        args = getattr(error, "args", ())
        if args:
            cleaned = tuple(redact_secrets(arg) for arg in args)
            if cleaned != args:
                error.args = cleaned
    except Exception:  # pragma: no cover - defensive; see docstring
        pass

    _redact_exception_chain(getattr(error, "__cause__", None), seen)
    _redact_exception_chain(getattr(error, "__context__", None), seen)


class SecretRedactingFilter(logging.Filter):
    """
    A logging filter that rewrites credentials out of a record before it is emitted.

    Returns True always -- it redacts, it never drops. Losing the record would
    hide the failure that made someone read the log in the first place.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            message = record.getMessage()
        except Exception:  # pragma: no cover - a broken format string is not ours to fix
            message = None

        if message is not None:
            redacted = redact_secrets(message)
            if redacted != message:
                # Collapse msg+args into the already-interpolated string: a
                # secret can live in either, and re-interpolating a redacted
                # msg against untouched args would put it back.
                record.msg = redacted
                record.args = None

        if record.exc_info and not record.exc_text:
            try:
                record.exc_text = _EXC_FORMATTER.formatException(record.exc_info)
            except Exception:  # pragma: no cover - defensive
                record.exc_text = None
        if record.exc_text:
            record.exc_text = redact_secrets(record.exc_text)

        if record.stack_info:
            record.stack_info = redact_secrets(record.stack_info)

        return True


def install_secret_redaction(*loggers: logging.Logger) -> int:
    """
    Install `SecretRedactingFilter` on the given loggers (default: root and the
    pipeline logger), and return the number of handlers newly covered.

    Idempotent -- safe to call from every entry point, which is the point: the
    scripts configure logging in three different ways (`utils.setup_logging`,
    `logging.basicConfig`, none at all), so the install has to be cheap enough
    to do unconditionally.

    Filters go on the handlers, which is what catches records from entsoe-py
    and urllib3 as well as ours. A filter is also placed on each logger itself
    so that a handler added *after* this call is still covered for records
    logged directly through it.
    """
    targets = loggers or (
        logging.getLogger(),
        logging.getLogger(PIPELINE_LOGGER_NAME),
    )

    covered = 0
    for target in targets:
        for handler in list(getattr(target, "handlers", [])):
            if not _has_filter(handler):
                handler.addFilter(SecretRedactingFilter())
                covered += 1
        if not _has_filter(target):
            target.addFilter(SecretRedactingFilter())

    return covered


def _has_filter(target: Any) -> bool:
    return any(isinstance(f, SecretRedactingFilter) for f in getattr(target, "filters", []))
