"""KoshaRequestError exposes the server's error model (#350)."""

from kosha_client.transport import KoshaRequestError


def test_error_fields_come_from_the_body():
    err = KoshaRequestError(
        503,
        "search unavailable",
        {
            "error": "search unavailable",
            "code": "unavailable",
            "retryable": True,
            "retry_after_ms": 1000,
            "request_id": "abc123",
            "details": {"reason": "control_store"},
        },
    )
    assert err.code == "unavailable"
    assert err.retryable is True
    assert err.retry_after_ms == 1000
    assert err.request_id == "abc123"
    assert "[unavailable]" in str(err)


def test_a_bare_body_still_works():
    err = KoshaRequestError(500, "boom", {"error": "boom"})
    assert err.code is None
    assert err.retryable is False
    assert err.request_id is None
    assert err.error == "boom"
