from __future__ import annotations

import pytest

from common.rate_limit import SlidingWindowRateLimiter


def test_rate_limiter_allows_within_limit_and_rejects_burst():
    limiter = SlidingWindowRateLimiter(limit=2, window_seconds=60, max_clients=4)

    assert limiter.allow("client-a", now=100.0) == (True, 0)
    assert limiter.allow("client-a", now=101.0) == (True, 0)
    allowed, retry_after = limiter.allow("client-a", now=102.0)

    assert allowed is False
    assert retry_after >= 1


def test_rate_limiter_expires_old_events():
    limiter = SlidingWindowRateLimiter(limit=1, window_seconds=10, max_clients=4)

    assert limiter.allow("client-a", now=100.0) == (True, 0)
    assert limiter.allow("client-a", now=109.0)[0] is False
    assert limiter.allow("client-a", now=110.1) == (True, 0)


@pytest.mark.parametrize(
    "value",
    ["", None],
)
def test_rate_limiter_requires_client_key(value):
    limiter = SlidingWindowRateLimiter(limit=1)
    with pytest.raises(ValueError):
        limiter.allow(value)  # type: ignore[arg-type]


def test_rate_limiter_can_be_disabled():
    limiter = SlidingWindowRateLimiter(limit=0)
    assert limiter.allow("client-a") == (True, 0)
