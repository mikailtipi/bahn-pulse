import httpx
import pytest

from bahn_pulse.clients.db_api import DbApiClient
from bahn_pulse.settings import DbApiCredentials

CREDS = DbApiCredentials(client_id="id-123", api_key="secret-456")


def make_client(responses: list[httpx.Response], sleeps: list[float]) -> DbApiClient:
    queue = iter(responses)
    seen_headers: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.append(request.headers)
        return next(queue)

    client = DbApiClient(
        CREDS,
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
        backoff_base_s=0.01,
    )
    client.seen_headers = seen_headers  # type: ignore[attr-defined]
    return client


def test_sends_auth_headers() -> None:
    client = make_client([httpx.Response(200, json={})], [])
    client.get("/x")
    headers = client.seen_headers[0]  # type: ignore[attr-defined]
    assert headers["DB-Client-Id"] == "id-123"
    assert headers["DB-Api-Key"] == "secret-456"


def test_retries_on_429_respecting_retry_after() -> None:
    sleeps: list[float] = []
    client = make_client(
        [httpx.Response(429, headers={"Retry-After": "2"}), httpx.Response(200, json={"ok": 1})],
        sleeps,
    )
    assert client.get("/x").json() == {"ok": 1}
    assert sleeps == [2.0]


def test_gives_up_after_max_retries() -> None:
    sleeps: list[float] = []
    client = make_client([httpx.Response(503)] * 5, sleeps)
    with pytest.raises(httpx.HTTPStatusError):
        client.get("/x")
    assert len(sleeps) == 4


def test_does_not_retry_client_errors() -> None:
    sleeps: list[float] = []
    client = make_client([httpx.Response(401)], sleeps)
    with pytest.raises(httpx.HTTPStatusError):
        client.get("/x")
    assert sleeps == []


def test_credentials_repr_hides_secrets() -> None:
    assert "secret-456" not in repr(CREDS)
    assert "id-123" not in repr(CREDS)
