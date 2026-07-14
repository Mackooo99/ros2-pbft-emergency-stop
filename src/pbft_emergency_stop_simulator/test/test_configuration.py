from pathlib import Path

import yaml

from pbft_emergency_stop_simulator.test_console.configuration import (
    materialize_scenario,
    scenario_compatibility,
    validate_configuration,
)


def config(n: int, f: int, initial_view: int = 0):
    return {
        "system": {
            "replica_count": n,
            "max_faulty": f,
            "initial_view": initial_view,
            "primary_policy": "round_robin",
        },
        "timing": {
            "progress_timeout_sec": 3.0,
            "decision_timeout_sec": 8.0,
            "heartbeat_period_sec": 0.5,
            "client_publish_delay_sec": 0.8,
        },
        "safety": {"allow_confirmed_release": False},
        "request": {"emergency_stop": True},
    }


def scenario(profile: str):
    return {
        "id": f"test-{profile}",
        "name": profile,
        "execution_mode": "configurable",
        "fault_profile": profile,
        "requirements": {"min_f": 1},
        "launch": {
            "package": "pbft_emergency_stop_simulator",
            "file": "pbft_configurable.launch.py",
        },
    }


def test_n4_f1_is_valid():
    result = validate_configuration(config(4, 1))
    assert result["valid"]
    assert result["derived"]["prepare_threshold"] == 2
    assert result["derived"]["commit_threshold"] == 3


def test_n7_f2_is_valid():
    result = validate_configuration(config(7, 2))
    assert result["valid"]
    assert result["derived"]["prepare_threshold"] == 4
    assert result["derived"]["commit_threshold"] == 5


def test_invalid_layout_is_rejected():
    result = validate_configuration(config(6, 2))
    assert not result["valid"]
    assert any("n = 3f + 1" in item for item in result["errors"])


def test_legacy_scenario_only_accepts_n4_f1():
    legacy = {"id": "legacy", "execution_mode": "legacy_n4_f1"}
    assert scenario_compatibility(legacy, validate_configuration(config(4, 1)))[
        "compatible"
    ]
    assert not scenario_compatibility(
        legacy, validate_configuration(config(7, 2))
    )["compatible"]


def test_maximum_fault_profile_creates_f_silent_backups(tmp_path: Path):
    validation = validate_configuration(config(7, 2, initial_view=6))
    output = tmp_path / "cluster.yaml"
    runtime = materialize_scenario(
        scenario("maximum_silent"), validation, output
    )
    cluster = yaml.safe_load(output.read_text())
    faulty = [
        item["node_id"]
        for item in cluster["replicas"]
        if item["behavior"] != "none"
    ]
    assert len(faulty) == 2
    assert 6 not in faulty
    assert runtime["actual_byzantine_count"] == 2
    assert runtime["within_fault_bound"] is True


def test_beyond_fault_bound_creates_f_plus_one_faults(tmp_path: Path):
    validation = validate_configuration(config(7, 2))
    output = tmp_path / "cluster.yaml"
    runtime = materialize_scenario(
        scenario("beyond_fault_bound_silent"), validation, output
    )
    assert runtime["actual_byzantine_count"] == 3
    assert runtime["within_fault_bound"] is False
    assert runtime["assertions"][0]["op"] == "not_exists"


def test_faulty_primary_profile_targets_current_primary(tmp_path: Path):
    validation = validate_configuration(config(7, 2, initial_view=3))
    output = tmp_path / "cluster.yaml"
    runtime = materialize_scenario(
        scenario("faulty_primary_recovery"), validation, output
    )
    cluster = yaml.safe_load(output.read_text())
    assert runtime["faulty_replica_ids"] == [3]
    assert cluster["replicas"][3]["behavior"] == "skip_pre_prepare"
    assert runtime["assertions"][2]["value"] == 4
