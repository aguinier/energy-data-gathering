"""ABL-86 -- the ENTSO-E credential was written to the ingest logs in cleartext.

entsoe-py puts the API key in the request URL as `securityToken=`, and
`requests.Response.raise_for_status()` builds its HTTPError message out of that
full URL. `ENTSOEClient._make_request` caught the exception and logged it, so
every failed request appended the credential to `logs/pipeline.log` and -- via
cron's `2>&1` -- `logs/cron_update.log`. Prod's copies are ~215 MB and ~70 MB,
owned by root, and exist to be tailed and pasted into diagnostics.

Every test here uses an obviously fake token. The real value is not in this
repo, this test, or any commit.

The HTTPError in the acceptance tests is not hand-written: it is produced by
`requests`' own `raise_for_status()` against a Response carrying the URL shape
recorded on the issue, so the thing under test is the message `requests`
really builds rather than one we assumed it builds.

Two properties are load-bearing and both are asserted:

  - The token is gone.
  - Everything ELSE in the URL survives -- documentType, the bidding-zone
    domain, periodStart. Scrubbing the whole URL would close this hole by
    making the log useless, and the log is how ABL-84 was diagnosed.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import log_redaction  # noqa: E402
from src.log_redaction import (  # noqa: E402
    REDACTED,
    SecretRedactingFilter,
    install_secret_redaction,
    redact_exception,
    redact_secrets,
    register_secret_value,
)

# Obviously fake, and long enough to clear MIN_REGISTERED_SECRET_LENGTH.
FAKE_TOKEN = "fake0000-1111-2222-3333-444455556666"

FAILING_URL = (
    "https://web-api.tp.entsoe.eu/api"
    "?documentType=A65&processType=A16"
    f"&outBiddingZone_Domain=10YAL-KESH-----5&securityToken={FAKE_TOKEN}"
    "&periodStart=202608080000&periodEnd=202608092300"
)

# The parts of the URL a human needs in order to diagnose an ingest gap.
DIAGNOSTIC_FRAGMENTS = (
    "documentType=A65",
    "processType=A16",
    "outBiddingZone_Domain=10YAL-KESH-----5",
    "periodStart=202608080000",
)


@pytest.fixture(autouse=True)
def isolated_redaction_state():
    """Registered values and installed filters are process-global; keep tests independent."""
    saved_values = set(log_redaction._registered_secret_values)
    root, pipeline = logging.getLogger(), logging.getLogger("entsoe_pipeline")
    saved_filters = {
        id(target): list(target.filters) for target in (root, pipeline)
    }
    try:
        yield
    finally:
        log_redaction._registered_secret_values.clear()
        log_redaction._registered_secret_values.update(saved_values)
        for target in (root, pipeline):
            target.filters = saved_filters[id(target)]


def http_error_from_requests(url: str = FAILING_URL) -> requests.HTTPError:
    """The real exception, built by requests itself from a 400 response."""
    response = requests.Response()
    response.status_code = 400
    response.reason = "Bad Request"
    response.url = url
    try:
        response.raise_for_status()
    except requests.HTTPError as error:
        return error
    raise AssertionError("raise_for_status did not raise")


def test_the_leak_this_fixes_is_real():
    """Guard the premise: requests really does put the whole URL in the message."""
    error = http_error_from_requests()
    assert FAKE_TOKEN in str(error)


# ---------------------------------------------------------------------------
# redact_secrets -- the query-parameter rule
# ---------------------------------------------------------------------------


def test_query_parameter_token_is_replaced():
    redacted = redact_secrets(str(http_error_from_requests()))

    assert FAKE_TOKEN not in redacted
    assert f"securityToken={REDACTED}" in redacted


def test_the_rest_of_the_url_survives():
    """A scrubbed log still has to be a usable log."""
    redacted = redact_secrets(str(http_error_from_requests()))

    for fragment in DIAGNOSTIC_FRAGMENTS:
        assert fragment in redacted


def test_parameter_name_matching_is_case_insensitive():
    assert FAKE_TOKEN not in redact_secrets(f"?SECURITYTOKEN={FAKE_TOKEN}&x=1")
    assert FAKE_TOKEN not in redact_secrets(f"?securitytoken={FAKE_TOKEN}&x=1")


def test_a_following_parameter_is_not_swallowed():
    """The value ends at `&`; the next parameter is data, not secret."""
    redacted = redact_secrets(f"a=1&securityToken={FAKE_TOKEN}&periodEnd=202608092300")

    assert redacted == f"a=1&securityToken={REDACTED}&periodEnd=202608092300"


def test_a_token_at_end_of_string_is_redacted():
    """ENTSO-E error text ends on the URL as often as not."""
    assert redact_secrets(f"for url: x?securityToken={FAKE_TOKEN}") == (
        f"for url: x?securityToken={REDACTED}"
    )


@pytest.mark.parametrize(
    "param", ["securityToken", "api_key", "apikey", "token", "password", "client_secret"]
)
def test_every_registered_parameter_name_is_covered(param):
    assert FAKE_TOKEN not in redact_secrets(f"https://x/api?{param}={FAKE_TOKEN}&y=2")


def test_params_dict_repr_is_redacted():
    """entsoe-py logs the params mapping at DEBUG before it makes the request."""
    line = (
        "Performing request to https://web-api.tp.entsoe.eu/api with params "
        f"{{'documentType': 'A65', 'securityToken': '{FAKE_TOKEN}'}}"
    )
    redacted = redact_secrets(line)

    assert FAKE_TOKEN not in redacted
    assert "'documentType': 'A65'" in redacted


def test_redaction_is_idempotent():
    once = redact_secrets(str(http_error_from_requests()))

    assert redact_secrets(once) == once


def test_non_strings_pass_through_untouched():
    assert redact_secrets(None) is None
    assert redact_secrets(42) == 42
    assert redact_secrets("") == ""


def test_ordinary_text_is_left_alone():
    line = "Fetched 492 rows for AL (2026-08-08 18:30) from energy_load"

    assert redact_secrets(line) == line


# ---------------------------------------------------------------------------
# register_secret_value -- the literal-value rule
# ---------------------------------------------------------------------------


def test_a_registered_value_is_scrubbed_in_any_shape():
    """The parameter list cannot anticipate every shape a key gets printed in."""
    assert register_secret_value(FAKE_TOKEN) is True

    assert redact_secrets(f"Using key {FAKE_TOKEN} for AL") == f"Using key {REDACTED} for AL"
    assert FAKE_TOKEN not in redact_secrets(f"Authorization: Bearer {FAKE_TOKEN}")


def test_a_short_value_is_refused():
    """Redacting a short, common string would blank unrelated text everywhere."""
    assert register_secret_value("abc") is False
    assert register_secret_value("") is False
    assert register_secret_value(None) is False

    assert redact_secrets("abc is fine here") == "abc is fine here"


def test_registering_the_key_covers_a_parameter_name_we_never_listed():
    register_secret_value(FAKE_TOKEN)

    assert FAKE_TOKEN not in redact_secrets(f"?entirelyUnknownParam={FAKE_TOKEN}")


# ---------------------------------------------------------------------------
# redact_exception -- the traceback path, which no logging filter sees
# ---------------------------------------------------------------------------


def test_exception_message_is_scrubbed_in_place():
    error = http_error_from_requests()

    redact_exception(error)

    assert FAKE_TOKEN not in str(error)
    assert "documentType=A65" in str(error)


def test_the_chained_cause_is_scrubbed_too():
    """`raise ENTSOEClientError(...) from e` prints the HTTPError underneath ours."""
    try:
        try:
            raise http_error_from_requests()
        except requests.HTTPError as cause:
            raise RuntimeError("API request failed") from cause
    except RuntimeError as wrapper:
        redact_exception(wrapper)
        assert FAKE_TOKEN not in str(wrapper.__cause__)


def test_structured_args_are_left_alone():
    error = ValueError("no secret here", 42, {"a": 1})

    redact_exception(error)

    assert error.args == ("no secret here", 42, {"a": 1})


def test_redacting_an_exception_never_raises():
    class ReadOnlyArgs(Exception):
        @property
        def args(self):
            return (f"?securityToken={FAKE_TOKEN}",)

    # Must not blow up on an exception type that refuses the rewrite.
    redact_exception(ReadOnlyArgs())


def test_a_self_referential_exception_chain_terminates():
    first, second = ValueError("a"), ValueError("b")
    first.__context__ = second
    second.__context__ = first

    redact_exception(first)  # would recurse forever without the seen-set


# ---------------------------------------------------------------------------
# SecretRedactingFilter -- covering all ~128 logger.error call sites at once
# ---------------------------------------------------------------------------


class CapturingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(self.format(record))


def test_filter_redacts_an_interpolated_message():
    handler = CapturingHandler()
    handler.addFilter(SecretRedactingFilter())
    logger = logging.getLogger("test_log_redaction.interpolated")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    try:
        # %-style args, not an f-string: the secret is in `args`, not in `msg`.
        logger.error("API request failed: %s", str(http_error_from_requests()))
    finally:
        logger.removeHandler(handler)

    assert handler.lines and FAKE_TOKEN not in handler.lines[0]
    assert "documentType=A65" in handler.lines[0]


def test_filter_redacts_an_attached_traceback():
    handler = CapturingHandler()
    handler.addFilter(SecretRedactingFilter())
    logger = logging.getLogger("test_log_redaction.traceback")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    try:
        try:
            raise http_error_from_requests()
        except requests.HTTPError:
            logger.exception("API request failed")
    finally:
        logger.removeHandler(handler)

    assert handler.lines and FAKE_TOKEN not in handler.lines[0]
    assert "Traceback" in handler.lines[0]


def test_filter_never_drops_a_record():
    """It redacts; it must not hide the failure someone opened the log to find."""
    record = logging.LogRecord(
        "x", logging.ERROR, __file__, 1, f"?securityToken={FAKE_TOKEN}", None, None
    )

    assert SecretRedactingFilter().filter(record) is True
    assert FAKE_TOKEN not in record.getMessage()


def test_install_is_idempotent():
    logger = logging.getLogger("test_log_redaction.install")
    logger.handlers = [CapturingHandler()]

    install_secret_redaction(logger)
    install_secret_redaction(logger)
    install_secret_redaction(logger)

    installed = [f for f in logger.handlers[0].filters if isinstance(f, SecretRedactingFilter)]
    assert len(installed) == 1


def test_install_covers_a_third_party_logger_through_the_handler():
    """entsoe-py and urllib3 log through their own loggers; the handler is shared."""
    handler = CapturingHandler()
    root = logging.getLogger()
    root.addHandler(handler)
    try:
        install_secret_redaction(root)
        third_party = logging.getLogger("entsoe.entsoe")
        third_party.setLevel(logging.DEBUG)
        third_party.error(f"Performing request with params securityToken={FAKE_TOKEN}")
    finally:
        root.removeHandler(handler)

    assert handler.lines and FAKE_TOKEN not in handler.lines[0]


# ---------------------------------------------------------------------------
# Acceptance -- the real client path that produced the leak
# ---------------------------------------------------------------------------


def test_make_request_logs_and_raises_without_the_token():
    from src.entsoe_client import ENTSOEClient, ENTSOEClientError

    handler = CapturingHandler()
    pipeline_logger = logging.getLogger("entsoe_pipeline")
    pipeline_logger.setLevel(logging.DEBUG)
    pipeline_logger.addHandler(handler)

    def failing_call():
        raise http_error_from_requests()

    try:
        client = ENTSOEClient(api_key=FAKE_TOKEN)
        with pytest.raises(ENTSOEClientError) as raised:
            client._make_request(failing_call)
    finally:
        pipeline_logger.removeHandler(handler)

    # What we log.
    assert handler.lines
    logged = "\n".join(handler.lines)
    assert FAKE_TOKEN not in logged
    assert "API request failed" in logged
    assert "outBiddingZone_Domain=10YAL-KESH-----5" in logged

    # What we re-raise, and what it was raised from -- both reach a traceback.
    assert FAKE_TOKEN not in str(raised.value)
    assert FAKE_TOKEN not in str(raised.value.__cause__)


def test_client_registers_its_own_key():
    from src.entsoe_client import ENTSOEClient

    ENTSOEClient(api_key=FAKE_TOKEN)

    # Registered, so the key is scrubbed even in a shape no parameter name covers.
    assert FAKE_TOKEN not in redact_secrets(f"some unforeseen line holding {FAKE_TOKEN}")


def test_format_error_redacts():
    assert FAKE_TOKEN not in utils_format_error(http_error_from_requests())


def utils_format_error(error):
    import utils

    return utils.format_error(error)
