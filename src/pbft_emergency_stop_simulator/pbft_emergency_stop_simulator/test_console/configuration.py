"""PBFT system configuration validation and scenario materialization."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


SUPPORTED_PRIMARY_POLICIES = {"round_robin"}
CONFIGURABLE_EXECUTION_MODE = "configurable"
LEGACY_EXECUTION_MODE = "legacy_n4_f1"


def default_configuration() -> dict[str, Any]:
    """Return the default configuration shown by the web wizard."""
    return {
        "system": {
            "replica_count": 4,
            "max_faulty": 1,
            "initial_view": 0,
            "primary_policy": "round_robin",
        },
        "timing": {
            "progress_timeout_sec": 3.0,
            "decision_timeout_sec": 8.0,
            "heartbeat_period_sec": 0.5,
            "client_publish_delay_sec": 0.8,
        },
        "safety": {
            "allow_confirmed_release": False,
        },
        "request": {
            "emergency_stop": True,
        },
    }


def validate_configuration(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize and validate a browser-supplied PBFT configuration."""
    merged = default_configuration()
    if raw:
        _deep_merge(merged, raw)

    errors: list[str] = []
    warnings: list[str] = []

    try:
        n = int(merged["system"]["replica_count"])
        f = int(merged["system"]["max_faulty"])
        initial_view = int(merged["system"]["initial_view"])
        primary_policy = str(merged["system"]["primary_policy"]).strip()
        progress_timeout = float(merged["timing"]["progress_timeout_sec"])
        decision_timeout = float(merged["timing"]["decision_timeout_sec"])
        heartbeat = float(merged["timing"]["heartbeat_period_sec"])
        client_delay = float(merged["timing"]["client_publish_delay_sec"])
        allow_release = bool(merged["safety"]["allow_confirmed_release"])
        emergency_stop = bool(merged["request"]["emergency_stop"])
    except (KeyError, TypeError, ValueError) as exc:
        return {
            "valid": False,
            "errors": [f"Configuration contains an invalid value: {exc}"],
            "warnings": [],
            "configuration": merged,
            "derived": {},
        }

    if n < 4:
        errors.append("Replica count n must be at least 4.")
    if n > 31:
        errors.append("Replica count n must not exceed 31 in the test console.")
    if f < 1:
        errors.append("Maximum faulty replicas f must be at least 1.")
    if f > 10:
        errors.append("Maximum faulty replicas f must not exceed 10.")

    # The current safety supervisor validates the canonical PBFT layout exactly.
    expected_n = 3 * f + 1
    if n != expected_n:
        errors.append(
            f"The current simulator requires n = 3f + 1; for f={f}, n must be {expected_n}."
        )

    if initial_view < 0:
        errors.append("Initial view must be non-negative.")
    if primary_policy not in SUPPORTED_PRIMARY_POLICIES:
        errors.append("Only the round_robin primary policy is currently supported.")
    if progress_timeout <= 0.0:
        errors.append("Progress timeout must be positive.")
    if decision_timeout <= 0.0:
        errors.append("Safety decision timeout must be positive.")
    if heartbeat <= 0.0:
        errors.append("Safety heartbeat period must be positive.")
    if client_delay < 0.0:
        errors.append("Client publish delay must be non-negative.")
    if decision_timeout <= progress_timeout:
        warnings.append(
            "Safety decision timeout is not greater than the progress timeout; "
            "view-change recovery may not finish before FAIL_SAFE_STOP."
        )
    if allow_release:
        warnings.append(
            "Confirmed release is enabled. Keep this disabled for the emergency-stop demonstration."
        )
    if not emergency_stop:
        warnings.append(
            "The request asks to release emergency stop. Most supplied scenarios expect emergency_stop=true."
        )

    normalized = {
        "system": {
            "replica_count": n,
            "max_faulty": f,
            "initial_view": initial_view,
            "primary_policy": primary_policy,
        },
        "timing": {
            "progress_timeout_sec": progress_timeout,
            "decision_timeout_sec": decision_timeout,
            "heartbeat_period_sec": heartbeat,
            "client_publish_delay_sec": client_delay,
        },
        "safety": {
            "allow_confirmed_release": allow_release,
        },
        "request": {
            "emergency_stop": emergency_stop,
        },
    }

    primary_id = initial_view % n if n > 0 else 0
    derived = {
        "primary_id": primary_id,
        "prepare_threshold": 2 * f,
        "commit_threshold": 2 * f + 1,
        "view_change_threshold": 2 * f + 1,
        "new_view_proof_threshold": 2 * f + 1,
        "legacy_compatible": n == 4 and f == 1 and initial_view == 0,
    }

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "configuration": normalized,
        "derived": derived,
    }


def scenario_compatibility(
    scenario: dict[str, Any],
    validation: dict[str, Any],
) -> dict[str, Any]:
    """Return whether a scenario can run with the selected configuration."""
    if not validation.get("valid"):
        return {"compatible": False, "reason": "System configuration is invalid."}

    mode = scenario.get("execution_mode", LEGACY_EXECUTION_MODE)
    config = validation["configuration"]
    f = int(config["system"]["max_faulty"])

    if mode == LEGACY_EXECUTION_MODE:
        if validation["derived"]["legacy_compatible"]:
            return {"compatible": True, "reason": "Uses the validated n=4, f=1 launch file."}
        return {
            "compatible": False,
            "reason": "This legacy scenario is fixed to n=4, f=1, initial view 0.",
        }

    if mode != CONFIGURABLE_EXECUTION_MODE:
        return {"compatible": False, "reason": f"Unsupported execution mode: {mode}"}

    requirements = scenario.get("requirements", {})
    min_f = int(requirements.get("min_f", 1))
    if f < min_f:
        return {
            "compatible": False,
            "reason": f"This scenario requires f >= {min_f}.",
        }

    return {"compatible": True, "reason": "Generated from the active PBFT configuration."}


def materialize_scenario(
    scenario: dict[str, Any],
    validation: dict[str, Any],
    cluster_config_path: Path,
) -> dict[str, Any]:
    """Create a concrete runtime scenario from a base scenario and system config."""
    compatibility = scenario_compatibility(scenario, validation)
    if not compatibility["compatible"]:
        raise ValueError(
            f"Scenario {scenario.get('id')} is incompatible: {compatibility['reason']}"
        )

    runtime = deepcopy(scenario)
    runtime["system_configuration"] = deepcopy(validation["configuration"])
    runtime["derived_configuration"] = deepcopy(validation["derived"])

    if runtime.get("execution_mode", LEGACY_EXECUTION_MODE) == LEGACY_EXECUTION_MODE:
        return runtime

    config = validation["configuration"]
    derived = validation["derived"]
    system = config["system"]
    timing = config["timing"]
    n = int(system["replica_count"])
    f = int(system["max_faulty"])
    initial_view = int(system["initial_view"])
    primary_id = int(derived["primary_id"])
    quorum = int(derived["commit_threshold"])
    profile = str(runtime.get("fault_profile", "normal"))

    replicas = [
        {
            "node_id": node_id,
            "behavior": "none",
            "enable_progress_timeout": False,
            "progress_timeout_sec": timing["progress_timeout_sec"],
            "prepare_delay_sec": 4.0,
            "commit_delay_sec": 4.0,
            "duplicate_message_count": 3,
        }
        for node_id in range(n)
    ]

    faulty_ids: list[int] = []
    expected_view = initial_view
    expect_decision = True

    backup_candidates = [
        node_id for node_id in reversed(range(n)) if node_id != primary_id
    ]

    if profile == "normal":
        pass
    elif profile == "one_silent":
        faulty_ids = backup_candidates[:1]
        for node_id in faulty_ids:
            replicas[node_id]["behavior"] = "silent"
    elif profile == "maximum_silent":
        faulty_ids = backup_candidates[:f]
        for node_id in faulty_ids:
            replicas[node_id]["behavior"] = "silent"
    elif profile == "beyond_fault_bound_silent":
        faulty_ids = backup_candidates[: f + 1]
        for node_id in faulty_ids:
            replicas[node_id]["behavior"] = "silent"
        expect_decision = False
    elif profile == "faulty_primary_recovery":
        faulty_ids = [primary_id]
        replicas[primary_id]["behavior"] = "skip_pre_prepare"
        for item in replicas:
            if item["node_id"] != primary_id:
                item["enable_progress_timeout"] = True
        expected_view = initial_view + 1
    else:
        raise ValueError(f"Unknown configurable fault profile: {profile}")

    cluster_config = {
        "system": deepcopy(system),
        "timing": deepcopy(timing),
        "replicas": replicas,
    }
    cluster_config_path.write_text(
        yaml.safe_dump(cluster_config, sort_keys=False), encoding="utf-8"
    )

    runtime["replica_count"] = n
    runtime["max_faulty"] = f
    runtime["actual_byzantine_count"] = len(faulty_ids)
    runtime["faulty_replica_ids"] = faulty_ids
    runtime["within_fault_bound"] = len(faulty_ids) <= f
    runtime["launch"] = {
        "package": "pbft_emergency_stop_simulator",
        "file": "pbft_configurable.launch.py",
        "arguments": {"config_file": str(cluster_config_path)},
    }
    runtime["supervisor"] = {
        "enabled": True,
        "parameters": {
            "replica_count": n,
            "max_faulty": f,
            "decision_timeout_sec": timing["decision_timeout_sec"],
            "allow_confirmed_release": config["safety"]["allow_confirmed_release"],
            "heartbeat_period_sec": timing["heartbeat_period_sec"],
        },
    }
    runtime["client"] = {
        "enabled": True,
        "start_delay_sec": 0.8,
        "parameters": {
            "primary_id": primary_id,
            "current_view": initial_view,
            "emergency_stop": config["request"]["emergency_stop"],
            "request_id": f"web-{runtime['id']}-n{n}-f{f}",
            "publish_delay_sec": timing["client_publish_delay_sec"],
        },
    }

    common_success_assertions = [
        {
            "id": "decision_received",
            "label": "Confirmed PBFT decision was published",
            "path": "decision.committed",
            "op": "eq",
            "value": True,
        },
        {
            "id": "confirmation_quorum",
            "label": f"Decision has at least {quorum} distinct confirmations",
            "path": "decision.confirmation_count",
            "op": "gte",
            "value": quorum,
        },
        {
            "id": "decision_view",
            "label": f"Decision was reached in view {expected_view}",
            "path": "decision.view",
            "op": "eq",
            "value": expected_view,
        },
        {
            "id": "safety_confirmed",
            "label": "Safety supervisor reached CONFIRMED_STOP",
            "path": "safety.state",
            "op": "eq",
            "value": "CONFIRMED_STOP",
        },
        {
            "id": "output_active",
            "label": "Effective emergency-stop output is active",
            "path": "safety.emergency_stop",
            "op": "eq",
            "value": True,
        },
    ]

    if expect_decision:
        runtime["complete_when"] = {
            "mode": "all",
            "conditions": [
                {"path": "decision.committed", "op": "eq", "value": True},
                {"path": "safety.state", "op": "eq", "value": "CONFIRMED_STOP"},
            ],
        }
        runtime["assertions"] = common_success_assertions
        if profile == "normal":
            runtime["assertions"].append(
                {
                    "id": "no_view_change",
                    "label": "Normal execution did not trigger VIEW-CHANGE",
                    "path": "counts.view_change",
                    "op": "eq",
                    "value": 0,
                }
            )
        if profile == "faulty_primary_recovery":
            runtime["assertions"].extend(
                [
                    {
                        "id": "view_change_quorum",
                        "label": f"At least {quorum} VIEW-CHANGE messages were observed",
                        "path": "counts.view_change",
                        "op": "gte",
                        "value": quorum,
                    },
                    {
                        "id": "new_view",
                        "label": "A NEW-VIEW message was published",
                        "path": "counts.new_view",
                        "op": "gte",
                        "value": 1,
                    },
                    {
                        "id": "new_primary",
                        "label": "Round-robin primary advanced after recovery",
                        "path": "primary_id",
                        "op": "eq",
                        "value": expected_view % n,
                    },
                ]
            )
        for node_id in faulty_ids:
            if replicas[node_id]["behavior"] == "silent":
                runtime["assertions"].extend(
                    [
                        {
                            "id": f"silent_no_prepare_{node_id}",
                            "label": f"Silent replica {node_id} sent no PREPARE",
                            "path": f"counts.prepare_by_sender.{node_id}",
                            "op": "not_exists",
                        },
                        {
                            "id": f"silent_no_commit_{node_id}",
                            "label": f"Silent replica {node_id} sent no COMMIT",
                            "path": f"counts.commit_by_sender.{node_id}",
                            "op": "not_exists",
                        },
                    ]
                )
        runtime["timeout_sec"] = max(
            float(runtime.get("timeout_sec", 15.0)),
            timing["decision_timeout_sec"] + 3.0,
            timing["progress_timeout_sec"] + 7.0,
        )
    else:
        runtime["complete_when"] = {
            "mode": "all",
            "conditions": [
                {"path": "safety.state", "op": "eq", "value": "FAIL_SAFE_STOP"},
                {"path": "safety.emergency_stop", "op": "eq", "value": True},
            ],
        }
        runtime["assertions"] = [
            {
                "id": "no_decision",
                "label": "No PBFT decision was produced beyond the fault bound",
                "path": "decision.committed",
                "op": "not_exists",
            },
            {
                "id": "fail_safe",
                "label": "Safety supervisor entered FAIL_SAFE_STOP",
                "path": "safety.state",
                "op": "eq",
                "value": "FAIL_SAFE_STOP",
            },
            {
                "id": "output_active",
                "label": "Emergency-stop output remained active",
                "path": "safety.emergency_stop",
                "op": "eq",
                "value": True,
            },
        ]
        runtime["timeout_sec"] = timing["decision_timeout_sec"] + 2.0

    return runtime


def _deep_merge(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = deepcopy(value)
