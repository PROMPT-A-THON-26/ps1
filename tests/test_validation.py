from __future__ import annotations

import pytest

from common.validation import MAX_OBJECT_NAME_LENGTH, normalize_object_name


def test_object_name_is_trimmed_and_preserved():
    assert normalize_object_name("  report.pdf  ") == "report.pdf"


@pytest.mark.parametrize(
    "value",
    ["", "   ", "report\x00.pdf", "report\n.pdf", "report\t.pdf"],
)
def test_object_name_rejects_empty_and_control_characters(value: str):
    with pytest.raises(ValueError):
        normalize_object_name(value)


def test_object_name_rejects_oversized_values():
    with pytest.raises(ValueError):
        normalize_object_name("x" * (MAX_OBJECT_NAME_LENGTH + 1))


def test_object_name_requires_string():
    with pytest.raises(ValueError):
        normalize_object_name(None)  # type: ignore[arg-type]
