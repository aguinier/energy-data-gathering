"""ENTSOEClient._make_request retry policy (ABL-665).

The defect these tests pin: the tenacity filter on `_make_request` named the
*builtin* `ConnectionError`/`TimeoutError`. `requests` raises neither --
`requests.exceptions.ConnectionError` descends from `OSError`, `Timeout` is not
a `TimeoutError`, and `HTTPError` is neither. So `MAX_RETRIES = 3` retried
almost nothing an HTTP client produces, and the 484 HTTP 503s of 2026-08-06
13:30 UTC were retried zero times.

Widening that tuple to the `requests` types would NOT have fixed it. The body of
`_make_request` catches `Exception` and re-raises `ENTSOEClientError`, so the
transport exception never reaches the decorator at all. Measured on the
pre-ABL-665 code, a 503 and a `requests.ConnectionError` each got exactly 1
attempt. The fix classifies at the raise site and retries on the wrapper type;
`test_a_transport_error_reaches_the_decorator_wrapped` is the regression pin for
that second half, because it is the one a "just fix the tuple" patch fails.

No test here sleeps for real: `wait_exponential` is patched out where attempt
counts are asserted, so the suite does not pay ABL-665's own backoff.
"""

import logging
from http.client import RemoteDisconnected
from socket import gaierror

import pytest
import requests

import config
from src.entsoe_client import (
    ENTSOEClient,
    ENTSOEClientError,
    ENTSOENoDataError,
    ENTSOETransientError,
    is_transient_upstream_error,
)
from entsoe.exceptions import NoMatchingDataError, InvalidPSRTypeError, PaginationError


FAKE_TOKEN = "fake-token-for-tests"


def _http_error(status):
    """A `requests.HTTPError` shaped like the one `raise_for_status` raises."""
    response = requests.Response()
    response.status_code = status
    response.url = f"https://web-api.tp.entsoe.eu/api?securityToken={FAKE_TOKEN}"
    return requests.exceptions.HTTPError(f"{status} Error", response=response)


@pytest.fixture
def client(monkeypatch):
    """A client with the rate limiter and the retry backoff neutralised.

    `_rate_limit` runs once per attempt by design; sleeping through it would
    make every multi-attempt test pay for it.
    """
    c = ENTSOEClient(api_key=FAKE_TOKEN)
    monkeypatch.setattr(c, "_rate_limit", lambda: None)
    return c


def _counting(exc):
    """A fake API method that always raises `exc`, recording each call."""
    calls = []

    def method(*args, **kwargs):
        calls.append(1)
        raise exc

    method.__name__ = "fake_query"
    return method, calls


@pytest.fixture(autouse=True)
def no_real_backoff(monkeypatch):
    """Make tenacity's wait a no-op so attempt counts cost no wall clock."""
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda _seconds: None)


# ---------------------------------------------------------------------------
# The pure classifier
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", [500, 502, 503, 504, 599, 429])
def test_5xx_and_429_are_transient(status):
    assert is_transient_upstream_error(_http_error(status)) is True


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422, 499])
def test_4xx_is_not_transient(status):
    """A bad request stays bad. 401 especially: retrying re-presents a
    credential that will not have become correct."""
    assert is_transient_upstream_error(_http_error(status)) is False


def test_http_error_without_a_response_is_not_transient():
    """No status to judge by -- guessing 'retry' is how a 4xx gets into the
    budget."""
    assert is_transient_upstream_error(requests.exceptions.HTTPError("no response")) is False


@pytest.mark.parametrize(
    "exc",
    [
        requests.exceptions.ConnectionError("refused"),
        requests.exceptions.Timeout("timed out"),
        requests.exceptions.ReadTimeout("read timed out"),
        requests.exceptions.ConnectTimeout("connect timed out"),
        gaierror("name resolution failed"),
        RemoteDisconnected("server closed connection"),
    ],
)
def test_transport_failures_are_transient(exc):
    assert is_transient_upstream_error(exc) is True


@pytest.mark.parametrize(
    "exc",
    [
        # RequestExceptions that are programming errors, not weather.
        requests.exceptions.MissingSchema("no scheme"),
        requests.exceptions.InvalidURL("bad url"),
        # entsoe-py's own answers.
        NoMatchingDataError(),
        InvalidPSRTypeError(),
        PaginationError("too many documents"),
        ValueError("bad argument"),
    ],
)
def test_permanent_failures_are_not_transient(exc):
    assert is_transient_upstream_error(exc) is False


def test_builtin_connection_error_is_not_what_requests_raises():
    """The original defect, stated as an assertion.

    If this ever fails, `requests` changed its hierarchy and the ABL-665
    reasoning needs revisiting.
    """
    assert not issubclass(requests.exceptions.ConnectionError, ConnectionError)
    assert not issubclass(requests.exceptions.Timeout, TimeoutError)
    assert not issubclass(requests.exceptions.HTTPError, (ConnectionError, TimeoutError))


# ---------------------------------------------------------------------------
# What _make_request actually does with them
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "exc",
    [
        _http_error(503),
        _http_error(500),
        _http_error(429),
        requests.exceptions.ConnectionError("refused"),
        requests.exceptions.Timeout("timed out"),
        gaierror("name resolution failed"),
        RemoteDisconnected("server closed connection"),
    ],
)
def test_transient_failures_are_retried_then_surface(client, exc):
    """Retried to the attempt budget, then raised -- not swallowed.

    Both halves matter: a retry that ends in a silent success would hide an
    outage from ABL-61's pass verdict.
    """
    method, calls = _counting(exc)

    with pytest.raises(ENTSOETransientError):
        client._make_request(method)

    assert len(calls) == config.MAX_RETRIES


def test_a_transport_error_reaches_the_decorator_wrapped(client):
    """The half of ABL-665 that a 'widen the tuple' patch does not fix.

    `_make_request`'s body wraps every failure before the decorator sees it, so
    what tenacity receives is our exception type, never `requests`'. The cause
    chain is preserved for the logs.
    """
    method, _calls = _counting(_http_error(503))

    with pytest.raises(ENTSOETransientError) as caught:
        client._make_request(method)

    assert isinstance(caught.value, ENTSOEClientError), "callers catch the base type"
    assert isinstance(caught.value.__cause__, requests.exceptions.HTTPError)
    assert caught.value.__cause__.response.status_code == 503


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_client_errors_are_not_retried(client, status):
    """One attempt, and the ordinary error type -- a 4xx must not spend the
    budget."""
    method, calls = _counting(_http_error(status))

    with pytest.raises(ENTSOEClientError) as caught:
        client._make_request(method)

    assert not isinstance(caught.value, ENTSOETransientError)
    assert len(calls) == 1


def test_no_matching_data_is_not_retried(client):
    """"No data" is an answer, not a failure. Retrying it turns every quiet
    country-hour into three requests."""
    method, calls = _counting(NoMatchingDataError())

    with pytest.raises(ENTSOENoDataError):
        client._make_request(method)

    assert len(calls) == 1


@pytest.mark.parametrize(
    "exc, expected",
    [
        (InvalidPSRTypeError(), ENTSOEClientError),
        (PaginationError("too many documents"), ENTSOEClientError),
        (ValueError("bad argument"), ENTSOEClientError),
    ],
)
def test_other_permanent_failures_are_not_retried(client, exc, expected):
    method, calls = _counting(exc)

    with pytest.raises(expected) as caught:
        client._make_request(method)

    assert not isinstance(caught.value, ENTSOETransientError)
    assert len(calls) == 1


def test_a_successful_call_is_made_once(client):
    """The retry wrapper must not change the happy path."""
    calls = []

    def method(*args, **kwargs):
        calls.append(1)
        return "payload"

    method.__name__ = "fake_query"

    assert client._make_request(method) == "payload"
    assert len(calls) == 1


def test_a_transient_failure_that_then_succeeds_returns_the_payload(client):
    """The point of the whole change: the 503 that ABL-665 found un-retried now
    resolves inside one pass instead of losing the country-hour."""
    calls = []

    def method(*args, **kwargs):
        calls.append(1)
        if len(calls) < config.MAX_RETRIES:
            raise _http_error(503)
        return "payload"

    method.__name__ = "fake_query"

    assert client._make_request(method) == "payload"
    assert len(calls) == config.MAX_RETRIES


def test_the_token_is_not_in_a_retried_exception(client, caplog):
    """ABL-86 redaction still applies on the new transient branch -- the 503's
    URL carries the API key."""
    method, _calls = _counting(_http_error(503))

    with caplog.at_level(logging.WARNING):
        with pytest.raises(ENTSOETransientError) as caught:
            client._make_request(method)

    assert FAKE_TOKEN not in str(caught.value)
    assert FAKE_TOKEN not in str(caught.value.__cause__)
    assert FAKE_TOKEN not in caplog.text


# ---------------------------------------------------------------------------
# The bound, and the second retry layer underneath us
# ---------------------------------------------------------------------------

def test_entsoe_py_own_retry_layer_is_collapsed(client):
    """entsoe-py retries requests.ConnectionError itself, 3x with a fixed 10s
    sleep, inside `EntsoeRawClient._base_request`.

    Left at that default it nests under ours and multiplies: 3 x 3 attempts and
    up to 90s of sleeping per failed request. ABL-665 collapses it to one
    attempt so there is a single, bounded retry layer. 1 is the floor -- at 0
    the library's `for _ in range(retry_count)` never runs and its `else`
    branch raises `None`.
    """
    assert config.ENTSOE_LIB_RETRY_COUNT == 1
    assert config.ENTSOE_LIB_RETRY_DELAY_SECONDS == 0
    for c in (client.client, client.raw_client):
        assert c.retry_count == 1
        assert c.retry_delay == 0


def test_the_added_wait_per_pass_stays_inside_the_cron_gap():
    """The bound ABL-665 has to hold, as arithmetic rather than prose.

    ~722 `_make_request` calls per full pass (7 types x 39 countries x 2 calls,
    plus 176 crossborder legs), each retried at most MAX_RETRIES times. ABL-61
    re-runs a failed pass up to 3 times. That total must stay well inside the
    5-6h cron gap, or a total outage delays the next pass instead of failing
    fast for it.
    """
    requests_per_pass = 722
    pass_runs = 3  # ABL-61: the pass plus its two retries

    # wait_exponential(multiplier=1, min=1, max=10) between attempts.
    waits = [
        min(max(2 ** i, config.RETRY_WAIT_MIN_SECONDS), config.RETRY_WAIT_MAX_SECONDS)
        for i in range(config.MAX_RETRIES - 1)
    ]
    added_seconds = requests_per_pass * sum(waits) * pass_runs

    assert added_seconds < 2 * 3600, (
        f"retry backoff would add {added_seconds / 3600:.1f}h across {pass_runs} "
        f"passes; the cron gap is 5-6h and the passes themselves need it"
    )
