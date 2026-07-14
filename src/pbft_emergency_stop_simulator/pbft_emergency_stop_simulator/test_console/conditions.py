"""Generic assertion evaluation for PBFT scenario snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


MISSING = object()


@dataclass(frozen=True)
class ConditionResult:
    """Result of one declarative condition."""

    passed: bool
    actual: Any
    expected: Any
    operator: str


def get_path(data: Any, path: str) -> Any:
    """Read a dot-separated path from nested dict/list data."""
    current = data

    if not path:
        return current

    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return MISSING
            current = current[part]
            continue

        if isinstance(current, list):
            try:
                index = int(part)
            except ValueError:
                return MISSING

            if index < 0 or index >= len(current):
                return MISSING

            current = current[index]
            continue

        return MISSING

    return current


def evaluate_condition(
    condition: dict[str, Any],
    state: dict[str, Any],
) -> ConditionResult:
    """Evaluate one condition against the current scenario state."""
    path = str(condition.get("path", ""))
    operator = str(condition.get("op", "eq"))
    expected = condition.get("value")
    actual = get_path(state, path)

    if operator.endswith("_path"):
        expected_path = str(condition.get("value_path", ""))
        expected = get_path(state, expected_path)

    if operator == "exists":
        passed = actual is not MISSING and actual is not None
    elif operator == "not_exists":
        passed = actual is MISSING or actual is None
    elif operator == "eq":
        passed = actual is not MISSING and actual == expected
    elif operator == "ne":
        passed = actual is MISSING or actual != expected
    elif operator == "gte":
        passed = actual is not MISSING and actual >= expected
    elif operator == "lte":
        passed = actual is not MISSING and actual <= expected
    elif operator == "gt":
        passed = actual is not MISSING and actual > expected
    elif operator == "lt":
        passed = actual is not MISSING and actual < expected
    elif operator == "contains":
        passed = actual is not MISSING and expected in actual
    elif operator == "not_contains":
        passed = actual is MISSING or expected not in actual
    elif operator == "in":
        passed = actual is not MISSING and actual in expected
    elif operator == "truthy":
        passed = actual is not MISSING and bool(actual)
    elif operator == "falsy":
        passed = actual is not MISSING and not bool(actual)
    elif operator == "set_eq":
        passed = (
            actual is not MISSING
            and isinstance(actual, (list, tuple, set))
            and set(actual) == set(expected)
        )
    elif operator == "length_eq":
        passed = actual is not MISSING and len(actual) == expected
    elif operator == "length_gte":
        passed = actual is not MISSING and len(actual) >= expected
    elif operator == "length_lte":
        passed = actual is not MISSING and len(actual) <= expected
    elif operator == "any_contains":
        passed = (
            actual is not MISSING
            and isinstance(actual, (list, tuple))
            and any(str(expected) in str(item) for item in actual)
        )
    elif operator == "all_contains":
        passed = (
            actual is not MISSING
            and isinstance(actual, (list, tuple))
            and bool(actual)
            and all(str(expected) in str(item) for item in actual)
        )
    elif operator == "eq_path":
        passed = (
            actual is not MISSING
            and expected is not MISSING
            and actual == expected
        )
    elif operator == "ne_path":
        passed = (
            actual is not MISSING
            and expected is not MISSING
            and actual != expected
        )
    elif operator == "lt_path":
        passed = (
            actual is not MISSING
            and expected is not MISSING
            and actual < expected
        )
    elif operator == "lte_path":
        passed = (
            actual is not MISSING
            and expected is not MISSING
            and actual <= expected
        )
    elif operator == "gt_path":
        passed = (
            actual is not MISSING
            and expected is not MISSING
            and actual > expected
        )
    elif operator == "gte_path":
        passed = (
            actual is not MISSING
            and expected is not MISSING
            and actual >= expected
        )
    else:
        raise ValueError(f"Unsupported condition operator: {operator}")

    public_actual = None if actual is MISSING else actual
    public_expected = None if expected is MISSING else expected

    return ConditionResult(
        passed=bool(passed),
        actual=public_actual,
        expected=public_expected,
        operator=operator,
    )


def evaluate_group(group: dict[str, Any], state: dict[str, Any]) -> bool:
    """Evaluate an all/any condition group."""
    conditions = list(group.get("conditions", []))
    mode = str(group.get("mode", "all"))

    if not conditions:
        return False

    results = [evaluate_condition(item, state).passed for item in conditions]

    if mode == "all":
        return all(results)
    if mode == "any":
        return any(results)

    raise ValueError(f"Unsupported condition group mode: {mode}")
