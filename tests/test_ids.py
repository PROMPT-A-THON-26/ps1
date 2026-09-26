from common.ids import new_request_id, normalize_request_id, validate_request_id

import pytest


def test_new_request_id_is_trace_safe():
    value = new_request_id()
    assert value.startswith("req_")
    assert len(value) <= 64
    assert normalize_request_id(value) == value


def test_request_id_rejects_log_control_and_oversized_values():
    assert normalize_request_id("req_ok-123") == "req_ok-123"
    assert normalize_request_id("req bad") != "req bad"
    assert normalize_request_id("req_\nforged") != "req_\nforged"
    assert normalize_request_id("x" * 65) != "x" * 65


def test_validate_request_id_is_strict():
    assert validate_request_id("req_ok-123") == "req_ok-123"
    for value in ("req bad", "req_\nforged", "x" * 65):
        with pytest.raises(ValueError):
            validate_request_id(value)
