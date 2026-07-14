"""Launch a PBFT cluster from a generated YAML configuration."""

from __future__ import annotations

from pathlib import Path

import yaml
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


PACKAGE_NAME = "pbft_emergency_stop_simulator"


def _launch_setup(context):
    config_path = Path(LaunchConfiguration("config_file").perform(context)).expanduser()
    if not config_path.is_file():
        raise RuntimeError(f"PBFT configuration file does not exist: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError("PBFT configuration file must contain a YAML mapping.")

    system = raw.get("system", {})
    timing = raw.get("timing", {})
    replicas = raw.get("replicas", [])

    replica_count = int(system["replica_count"])
    max_faulty = int(system["max_faulty"])
    current_view = int(system.get("initial_view", 0))
    primary_id = current_view % replica_count

    if len(replicas) != replica_count:
        raise RuntimeError(
            f"Expected {replica_count} replica entries, found {len(replicas)}."
        )

    nodes = []
    seen_ids = set()

    for replica in replicas:
        node_id = int(replica["node_id"])
        if node_id in seen_ids:
            raise RuntimeError(f"Duplicate replica node_id: {node_id}")
        seen_ids.add(node_id)

        behavior = str(replica.get("behavior", "none")).strip().lower()
        parameters = {
            "node_id": node_id,
            "primary_id": primary_id,
            "current_view": current_view,
            "replica_count": replica_count,
            "max_faulty": max_faulty,
            "is_byzantine": behavior != "none",
            "byzantine_behavior": behavior,
            "duplicate_message_count": int(
                replica.get("duplicate_message_count", 3)
            ),
            "prepare_delay_sec": float(replica.get("prepare_delay_sec", 4.0)),
            "commit_delay_sec": float(replica.get("commit_delay_sec", 4.0)),
            "enable_progress_timeout": bool(
                replica.get("enable_progress_timeout", False)
            ),
            "progress_timeout_sec": float(
                replica.get(
                    "progress_timeout_sec",
                    timing.get("progress_timeout_sec", 3.0),
                )
            ),
        }

        optional_parameters = (
            "manual_view_change_target",
            "manual_view_change_delay_sec",
        )
        for key in optional_parameters:
            if key in replica:
                parameters[key] = replica[key]

        nodes.append(
            Node(
                package=PACKAGE_NAME,
                executable="pbft_replica",
                name=f"pbft_node_{node_id}",
                output="screen",
                emulate_tty=True,
                parameters=[parameters],
            )
        )

    expected_ids = set(range(replica_count))
    if seen_ids != expected_ids:
        raise RuntimeError(
            "Replica IDs must cover the complete range 0..n-1; "
            f"expected={sorted(expected_ids)}, actual={sorted(seen_ids)}"
        )

    nodes.append(
        Node(
            package=PACKAGE_NAME,
            executable="pbft_monitor",
            name="pbft_monitor",
            output="screen",
            emulate_tty=True,
            parameters=[
                {
                    "replica_count": replica_count,
                    "max_faulty": max_faulty,
                }
            ],
        )
    )

    return nodes


def generate_launch_description() -> LaunchDescription:
    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "config_file",
                description="Absolute path to a generated PBFT cluster YAML file.",
            ),
            OpaqueFunction(function=_launch_setup),
        ]
    )
