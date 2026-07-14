"""Scenario catalog loading and validation."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from ament_index_python.packages import get_package_share_directory


PACKAGE_NAME = "pbft_emergency_stop_simulator"


def default_catalog_path() -> Path:
    """Return the installed scenario catalog path."""
    share = Path(get_package_share_directory(PACKAGE_NAME))
    return share / "config" / "scenario_catalog.yaml"


def load_catalog(path: str | Path | None = None) -> dict[str, Any]:
    """Load and minimally validate the scenario catalog."""
    catalog_path = Path(path) if path else default_catalog_path()

    with catalog_path.open("r", encoding="utf-8") as stream:
        raw = yaml.safe_load(stream)

    if not isinstance(raw, dict):
        raise ValueError("Scenario catalog must contain a YAML mapping.")

    defaults = raw.get("defaults", {})
    scenarios = raw.get("scenarios", [])

    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("Scenario catalog must contain at least one scenario.")

    resolved = []
    identifiers: set[str] = set()

    for item in scenarios:
        if not isinstance(item, dict):
            raise ValueError("Every scenario entry must be a mapping.")

        merged = deepcopy(defaults)
        _deep_merge(merged, item)

        scenario_id = str(merged.get("id", "")).strip()
        if not scenario_id:
            raise ValueError("Every scenario must define a non-empty id.")
        if scenario_id in identifiers:
            raise ValueError(f"Duplicate scenario id: {scenario_id}")

        launch = merged.get("launch", {})
        if not launch.get("package") or not launch.get("file"):
            raise ValueError(
                f"Scenario {scenario_id} must define launch.package and launch.file."
            )

        identifiers.add(scenario_id)
        resolved.append(merged)

    return {
        "version": raw.get("version", 1),
        "catalog_path": str(catalog_path),
        "scenarios": resolved,
    }


def scenario_map(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Index scenarios by ID."""
    return {str(item["id"]): item for item in catalog["scenarios"]}


def public_scenario(scenario: dict[str, Any]) -> dict[str, Any]:
    """Return fields safe and useful for the browser."""
    return {
        "id": scenario["id"],
        "name": scenario.get("name", scenario["id"]),
        "category": scenario.get("category", "other"),
        "description": scenario.get("description", ""),
        "timeout_sec": scenario.get("timeout_sec", 15.0),
        "tags": scenario.get("tags", []),
        "execution_mode": scenario.get("execution_mode", "legacy_n4_f1"),
        "fault_profile": scenario.get("fault_profile"),
        "requirements": scenario.get("requirements", {}),
    }


def _deep_merge(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = deepcopy(value)
