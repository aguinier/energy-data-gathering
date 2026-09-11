"""ENTSOEClient's HTTP timeout (ABL-668).

The defect: `EntsoePandasClient`/`EntsoeRawClient` were built without a
`timeout`, so every `session.get(..., timeout=None)` inside entsoe-py could
block forever on a connection that stopped answering. Nothing above it measures
time -- ABL-665's tenacity layer counts attempts, ABL-61's pass retry only fires
on a pass that finishes -- so one half-open socket stalled the whole pass.

It also made half of ABL-665 unreachable: `is_transient_upstream_error` retries
`requests.exceptions.Timeout`, and with no timeout that exception could never be
raised on the ENTSO-E path.

The mechanism tests below point entsoe-py at a loopback server that accepts the
connection and then says nothing, which is exactly what a half-open upstream
looks like from the client. They drive the real stack -- requests, entsoe-py's
own `@retry`, `_make_request`, tenacity -- with the timeout shrunk to a fraction
of a second, and each call runs under a deadline so a regression fails the test
instead of hanging CI.
"""

import socket
import threading
import time

import pandas as pd
import pytest
import requests

import entsoe.entsoe
from entsoe import EntsoePandasClient, EntsoeRawClient

import config
from src.entsoe_client import ENTSOEClient, ENTSOETransientError


FAKE_TOKEN = "fake-token-for-tests"
START = pd.Timestamp("2026-09-01", tz="UTC")
END = pd.Timestamp("2026-09-02", tz="UTC")

# Shrunk for the mechanism tests only; the production values are pinned below.
TEST_TIMEOUT = (2.0, 0.25)


class _MuteServer:
    """Accepts TCP connections on loopback and never finishes a response.

    With `reply=b""` it never sends a byte: the client connects, sends its
    request, and waits for a status line that never comes. With a partial
    `reply` it sends headers and part of the body, then stalls -- the other way
    a connection goes half-open, mid-transfer.
    """

    def __init__(self, reply: bytes = b""):
        self.reply = reply
        self.accepted = 0
        self._conns = []
        self._stop = threading.Event()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self._sock.settimeout(0.05)
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        while not self._stop.is_set():
            try:
                conn, _addr = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self.accepted += 1
            self._conns.append(conn)
            if self.reply:
                conn.sendall(self.reply)

    def accepted_after(self, expected, within_s=2.0):
        """The accept count, once it reaches `expected` or `within_s` passes.

        The kernel completes a TCP handshake before this thread gets round to
        accept(), so on a loaded runner the count can trail the client by a
        moment. Waiting for it keeps the assertion about attempts, not threads.
        """
        deadline = time.monotonic() + within_s
        while self.accepted < expected and time.monotonic() < deadline:
            time.sleep(0.01)
        return self.accepted

    def close(self):
        self._stop.set()
        self._thread.join(timeout=2)
        for conn in self._conns:
            conn.close()
        self._sock.close()


@pytest.fixture
def mute_server(monkeypatch):
    server = _MuteServer()
    monkeypatch.setattr(entsoe.entsoe, "URL", f"http://127.0.0.1:{server.port}/api")
    yield server
    server.close()


@pytest.fixture
def stalled_body_server(monkeypatch):
    body_start = b"<?xml version='1.0' encoding='UTF-8'?><GL_MarketDocument>"
    reply = (
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Type: application/xml\r\n"
        b"Content-Length: 100000\r\n"
        b"\r\n" + body_start
    )
    server = _MuteServer(reply=reply)
    monkeypatch.setattr(entsoe.entsoe, "URL", f"http://127.0.0.1:{server.port}/api")
    yield server
    server.close()


@pytest.fixture
def client(monkeypatch):
    """A real ENTSOEClient built with the shrunk timeout.

    The timeout is read from config at construction, so it is patched first.
    The rate limiter and tenacity's backoff are neutralised, as in the ABL-665
    tests, so attempt counts cost no wall clock beyond the timeouts themselves.
    """
    monkeypatch.setattr(config, "ENTSOE_HTTP_TIMEOUT", TEST_TIMEOUT)
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda _seconds: None)
    c = ENTSOEClient(api_key=FAKE_TOKEN)
    monkeypatch.setattr(c, "_rate_limit", lambda: None)
    return c


def _call_with_deadline(fn, deadline_s):
    """Run `fn` in a daemon thread; fail the test if it is still blocked.

    This is what keeps a regression from hanging the suite: without a timeout
    the call below never returns, and the thread is simply abandoned (the
    fixture's teardown closes the socket under it).
    """
    outcome = {}

    def run():
        try:
            outcome["value"] = fn()
        except BaseException as e:  # noqa: BLE001 -- handed back to the test
            outcome["error"] = e

    t0 = time.monotonic()
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(deadline_s)
    if thread.is_alive():
        pytest.fail(
            f"still blocked after {deadline_s}s against a server that never "
            f"answers -- no timeout reached the socket"
        )
    return outcome, time.monotonic() - t0


def _deadline():
    """Every attempt may spend the full read timeout; allow that plus slack."""
    return config.MAX_RETRIES * sum(TEST_TIMEOUT) + 5


# ---------------------------------------------------------------------------
# The mechanism, end to end
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("which", ["raw_client", "client"])
def test_a_silent_upstream_times_out_and_reaches_the_retry_branch(client, mute_server, which):
    """The acceptance criterion ABL-665 left open: a timeout now reaches
    `is_transient_upstream_error`'s Timeout branch, is retried to the attempt
    budget, and surfaces -- rather than blocking the pass forever.

    Both entsoe-py clients are covered: the with-metadata queries use the raw
    one for the XML and the pandas one for the frame, so either can be the
    request that hangs.
    """
    query = getattr(client, which).query_load
    outcome, elapsed = _call_with_deadline(
        lambda: client._make_request(query, "BE", start=START, end=END), _deadline()
    )

    err = outcome.get("error")
    assert isinstance(err, ENTSOETransientError), f"expected a transient failure, got {err!r}"
    assert isinstance(err.__cause__, requests.exceptions.ReadTimeout)
    # One fresh connection per attempt: tenacity retried, entsoe-py's own layer
    # (collapsed to one attempt by ABL-665) did not multiply it.
    assert mute_server.accepted_after(config.MAX_RETRIES) == config.MAX_RETRIES
    assert elapsed >= config.MAX_RETRIES * TEST_TIMEOUT[1] * 0.9


def test_a_body_that_stalls_mid_transfer_is_bounded_too(client, stalled_body_server):
    """The read timeout bounds every wait for bytes, not only the wait for the
    status line. A stall after the headers surfaces from `requests` as a
    ConnectionError wrapping urllib3's ReadTimeoutError -- a different type from
    the test above, and still classified transient, so it is retried the same
    way.
    """
    outcome, _elapsed = _call_with_deadline(
        lambda: client._make_request(client.raw_client.query_load, "BE", start=START, end=END),
        _deadline(),
    )

    err = outcome.get("error")
    assert isinstance(err, ENTSOETransientError), f"expected a transient failure, got {err!r}"
    assert isinstance(err.__cause__, requests.exceptions.ConnectionError)
    assert stalled_body_server.accepted_after(config.MAX_RETRIES) == config.MAX_RETRIES


def test_a_crossborder_leg_is_bounded_without_the_retry_layer(client, mute_server):
    """`query_crossborder_all` calls `self.client.query_crossborder_flows`
    directly, not through `_make_request`, so a leg gets ONE attempt and its
    error is logged per border. The timeout still applies, because it lives on
    the shared entsoe-py client rather than in the retry layer.
    """
    outcome, _elapsed = _call_with_deadline(
        lambda: client.client.query_crossborder_flows("BE", "NL", start=START, end=END),
        sum(TEST_TIMEOUT) + 5,
    )

    assert isinstance(outcome.get("error"), requests.exceptions.ReadTimeout)
    assert mute_server.accepted_after(1) == 1


# ---------------------------------------------------------------------------
# The wiring
# ---------------------------------------------------------------------------

def test_both_entsoe_py_clients_carry_the_configured_timeout():
    c = ENTSOEClient(api_key=FAKE_TOKEN)
    assert c.client.timeout == config.ENTSOE_HTTP_TIMEOUT
    assert c.raw_client.timeout == config.ENTSOE_HTTP_TIMEOUT


def test_the_configured_timeout_reaches_session_get(monkeypatch):
    """What entsoe-py stores is not the claim; what `session.get` receives is."""
    c = ENTSOEClient(api_key=FAKE_TOKEN)
    seen = []

    def fake_get(*args, **kwargs):
        seen.append(kwargs.get("timeout"))
        raise requests.exceptions.ReadTimeout("simulated")

    for lib_client in (c.client, c.raw_client):
        monkeypatch.setattr(lib_client.session, "get", fake_get)
        with pytest.raises(requests.exceptions.ReadTimeout):
            lib_client.query_load("BE", start=START, end=END)

    assert seen == [config.ENTSOE_HTTP_TIMEOUT, config.ENTSOE_HTTP_TIMEOUT]


def test_entsoe_py_still_defaults_to_no_timeout():
    """The library default this change exists to override.

    If this starts failing, entsoe-py began defaulting a timeout of its own.
    Ours still wins because we pass one explicitly, but the premise of ABL-668
    changed and the comment on config.ENTSOE_HTTP_TIMEOUT should say so.
    """
    assert EntsoeRawClient(api_key=FAKE_TOKEN).timeout is None
    assert EntsoePandasClient(api_key=FAKE_TOKEN).timeout is None


# ---------------------------------------------------------------------------
# The value, and the measurement it rests on
# ---------------------------------------------------------------------------

# From prod's cron_update.log, 2026-03-07..2026-09-11 (ABL-668). Each fetch was
# split at its document's createdDateTime, which ENTSO-E stamps on generation,
# so each half bounds one HTTP request. Re-measure before moving either value.
MEASURED_SLOWEST_SINGLE_REQUEST_S = 254.4        # ME price, 2026-09-09; of 137,361 fetches
MEASURED_SLOWEST_RESPONSE_OF_ANY_KIND_S = 346.0  # an HTTP 504 Gateway Time-out
MEASURED_SLOWEST_CONNECT_S = 0.061               # TCP 23ms + TLS 38ms, prod container
OBSERVED_STUCK_CONNECTION_S = 958                # BE week-ahead 2026-09-05, reset by the network
READ_HEADROOM = 2.0


def test_the_read_timeout_clears_every_measured_response_with_headroom():
    """A read timeout that fires on a slow success stores a gap nothing alarms
    on -- worse than the hang it replaces. So it must clear the slowest single
    request on record with room for a slower storm, and every response upstream
    has ever sent at all."""
    assert config.ENTSOE_READ_TIMEOUT_SECONDS >= READ_HEADROOM * MEASURED_SLOWEST_SINGLE_REQUEST_S
    assert config.ENTSOE_READ_TIMEOUT_SECONDS > MEASURED_SLOWEST_RESPONSE_OF_ANY_KIND_S


def test_the_read_timeout_fires_before_the_hangs_on_record_ended():
    """The other side of the band: a timeout longer than the stuck connections
    we have actually seen would not have fired on any of them."""
    assert config.ENTSOE_READ_TIMEOUT_SECONDS < OBSERVED_STUCK_CONNECTION_S


def test_the_connect_timeout_is_loose_against_the_measured_handshake():
    """Connect may be tight -- it is retried like any transport failure and
    wastes no server work -- but not so tight that three SYN retransmits
    (1+2+4s) or a slow handshake on a loaded day trip it."""
    assert config.ENTSOE_CONNECT_TIMEOUT_SECONDS >= 7
    assert config.ENTSOE_CONNECT_TIMEOUT_SECONDS >= 100 * MEASURED_SLOWEST_CONNECT_S
    assert config.ENTSOE_CONNECT_TIMEOUT_SECONDS < config.ENTSOE_READ_TIMEOUT_SECONDS


def test_the_timeout_is_the_connect_read_pair():
    assert config.ENTSOE_HTTP_TIMEOUT == (
        config.ENTSOE_CONNECT_TIMEOUT_SECONDS,
        config.ENTSOE_READ_TIMEOUT_SECONDS,
    )


def test_one_request_can_no_longer_eat_the_cron_gap():
    """The bound this change adds, as arithmetic rather than prose.

    One request whose every attempt stalls to the limit costs MAX_RETRIES x
    (connect + read) plus tenacity's waits; before ABL-668 it was unbounded.
    It must sit well inside the 5-6h gap between passes.

    Per request, not per pass: a pass in which EVERY request stalls is still
    not bounded by anything here.
    """
    waits = [
        min(max(2 ** i, config.RETRY_WAIT_MIN_SECONDS), config.RETRY_WAIT_MAX_SECONDS)
        for i in range(config.MAX_RETRIES - 1)
    ]
    worst = config.MAX_RETRIES * sum(config.ENTSOE_HTTP_TIMEOUT) + sum(waits)

    assert worst < 3600, (
        f"one stalled request could hold the pass for {worst / 60:.0f} min; "
        f"the cron gap is 5-6h and the rest of the pass needs it"
    )
