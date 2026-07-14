"""Unit tests for declarative scenario conditions."""

from pbft_emergency_stop_simulator.test_console.conditions import (
    evaluate_condition,
    evaluate_group,
    get_path,
)


STATE = {
    "counts": {
        "new_view": 1,
        "senders": [1, 2, 3],
    },
    "details": [
        "Valid PRE-PREPARE accepted.",
        "Replica intentionally skipped its PREPARE message.",
    ],
    "timing": {
        "commit": 10,
        "prepare": 20,
    },
    "decision": {
        "committed": True,
        "view": 1,
    },
}


def test_get_path_reads_nested_values() -> None:
    assert get_path(STATE, "decision.view") == 1


def test_basic_condition_operators() -> None:
    assert evaluate_condition(
        {"path": "decision.committed", "op": "eq", "value": True},
        STATE,
    ).passed
    assert evaluate_condition(
        {"path": "counts.new_view", "op": "gte", "value": 1},
        STATE,
    ).passed
    assert evaluate_condition(
        {"path": "missing", "op": "not_exists"},
        STATE,
    ).passed


def test_collection_operators() -> None:
    assert evaluate_condition(
        {"path": "counts.senders", "op": "set_eq", "value": [3, 2, 1]},
        STATE,
    ).passed
    assert evaluate_condition(
        {"path": "counts.senders", "op": "length_gte", "value": 3},
        STATE,
    ).passed
    assert evaluate_condition(
        {"path": "details", "op": "any_contains", "value": "skipped"},
        STATE,
    ).passed


def test_path_comparison_operator() -> None:
    assert evaluate_condition(
        {
            "path": "timing.commit",
            "op": "lt_path",
            "value_path": "timing.prepare",
        },
        STATE,
    ).passed


def test_condition_groups() -> None:
    assert evaluate_group(
        {
            "mode": "all",
            "conditions": [
                {"path": "decision.committed", "op": "eq", "value": True},
                {"path": "decision.view", "op": "eq", "value": 1},
            ],
        },
        STATE,
    )
