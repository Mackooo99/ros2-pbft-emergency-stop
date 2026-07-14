"""Launch a fully configurable PBFT emergency-stop demonstration.

This launch file is intended for interactive demonstrations and oral defenses.
It starts a configurable replica cluster and can optionally start the monitor,
safety supervisor, and a single client request.
"""

from __future__ import annotations

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, LogInfo, OpaqueFunction, TimerAction
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


PACKAGE_NAME = "pbft_emergency_stop_simulator"

ALLOWED_BEHAVIORS = {
    "none",
    "silent",
    "bad_digest",
    "duplicate",
    "equivocation",
    "skip_prepare",
    "skip_commit",
    "delayed_prepare",
    "delayed_commit",
    "early_commit",
    "wrong_sequence",
    "wrong_view",
    "wrong_value",
    "invalid_sender",
    "skip_pre_prepare",
}


def _text(context, name: str) -> str:
    return LaunchConfiguration(name).perform(context).strip()


def _as_bool(value: str, name: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(
        f"Launch argument '{name}' must be true/false, received: {value!r}"
    )


def _as_int(value: str, name: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Launch argument '{name}' must be an integer, received: {value!r}"
        ) from exc


def _as_float(value: str, name: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise RuntimeError(
            f"Launch argument '{name}' must be numeric, received: {value!r}"
        ) from exc


def _parse_int_list(value: str, name: str) -> list[int]:
    if not value or value.lower() == "none":
        return []

    result: list[int] = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        result.append(_as_int(token, name))
    return result


def _parse_fault_map(
    faulty_nodes_text: str,
    faulty_behaviors_text: str,
    replica_count: int,
) -> dict[int, str]:
    faulty_nodes = _parse_int_list(faulty_nodes_text, "faulty_nodes")

    if len(set(faulty_nodes)) != len(faulty_nodes):
        raise RuntimeError("faulty_nodes must not contain duplicate replica IDs.")

    invalid_nodes = [
        node_id for node_id in faulty_nodes if not 0 <= node_id < replica_count
    ]
    if invalid_nodes:
        raise RuntimeError(
            "faulty_nodes contains IDs outside the range "
            f"0..{replica_count - 1}: {invalid_nodes}"
        )

    if not faulty_nodes:
        return {}

    behaviors = [
        token.strip().lower()
        for token in faulty_behaviors_text.split(",")
        if token.strip()
    ]

    if len(behaviors) == 1 and len(faulty_nodes) > 1:
        behaviors *= len(faulty_nodes)

    if len(behaviors) != len(faulty_nodes):
        raise RuntimeError(
            "faulty_behaviors must contain either one behavior for all faulty "
            "replicas or one behavior per faulty replica. "
            f"nodes={faulty_nodes}, behaviors={behaviors}"
        )

    unsupported = sorted(set(behaviors) - ALLOWED_BEHAVIORS)
    if unsupported:
        raise RuntimeError(
            f"Unsupported Byzantine behavior(s): {unsupported}. "
            f"Allowed values: {sorted(ALLOWED_BEHAVIORS)}"
        )

    return dict(zip(faulty_nodes, behaviors))


def _progress_timeout_nodes(
    specification: str,
    replica_count: int,
    primary_id: int,
    faulty_nodes: set[int],
) -> set[int]:
    normalized = specification.strip().lower()

    if normalized in {"", "none"}:
        return set()
    if normalized == "all":
        return set(range(replica_count))
    if normalized == "backups":
        return set(range(replica_count)) - {primary_id}
    if normalized == "correct":
        return set(range(replica_count)) - faulty_nodes
    if normalized == "faulty":
        return set(faulty_nodes)

    nodes = set(_parse_int_list(specification, "progress_timeout_nodes"))
    invalid_nodes = [node_id for node_id in nodes if not 0 <= node_id < replica_count]
    if invalid_nodes:
        raise RuntimeError(
            "progress_timeout_nodes contains IDs outside the range "
            f"0..{replica_count - 1}: {invalid_nodes}"
        )
    return nodes


def _launch_setup(context):
    replica_count = _as_int(_text(context, "replica_count"), "replica_count")
    max_faulty = _as_int(_text(context, "max_faulty"), "max_faulty")
    initial_view = _as_int(_text(context, "initial_view"), "initial_view")

    if max_faulty < 0:
        raise RuntimeError("max_faulty must be non-negative.")
    if replica_count != 3 * max_faulty + 1:
        raise RuntimeError(
            "This simulator currently requires n = 3f + 1. "
            f"Received n={replica_count}, f={max_faulty}; "
            f"expected n={3 * max_faulty + 1}."
        )
    if initial_view < 0:
        raise RuntimeError("initial_view must be non-negative.")

    primary_id = initial_view % replica_count

    fault_map = _parse_fault_map(
        _text(context, "faulty_nodes"),
        _text(context, "faulty_behaviors"),
        replica_count,
    )

    progress_nodes = _progress_timeout_nodes(
        _text(context, "progress_timeout_nodes"),
        replica_count,
        primary_id,
        set(fault_map),
    )

    progress_timeout_sec = _as_float(
        _text(context, "progress_timeout_sec"), "progress_timeout_sec"
    )
    prepare_delay_sec = _as_float(
        _text(context, "prepare_delay_sec"), "prepare_delay_sec"
    )
    commit_delay_sec = _as_float(
        _text(context, "commit_delay_sec"), "commit_delay_sec"
    )
    duplicate_message_count = _as_int(
        _text(context, "duplicate_message_count"), "duplicate_message_count"
    )
    new_view_pre_prepare_delay_sec = _as_float(
        _text(context, "new_view_pre_prepare_delay_sec"),
        "new_view_pre_prepare_delay_sec",
    )
    status_publish_period_sec = _as_float(
        _text(context, "status_publish_period_sec"), "status_publish_period_sec"
    )
    protocol_qos_depth = _as_int(
        _text(context, "protocol_qos_depth"), "protocol_qos_depth"
    )
    status_qos_depth = _as_int(
        _text(context, "status_qos_depth"), "status_qos_depth"
    )

    manual_node_text = _text(context, "manual_view_change_node")
    manual_node = -1 if manual_node_text.lower() in {"", "none", "-1"} else _as_int(
        manual_node_text, "manual_view_change_node"
    )
    manual_target = _as_int(
        _text(context, "manual_view_change_target"), "manual_view_change_target"
    )
    manual_delay = _as_float(
        _text(context, "manual_view_change_delay_sec"),
        "manual_view_change_delay_sec",
    )

    if manual_node >= replica_count:
        raise RuntimeError(
            "manual_view_change_node must be -1/none or a valid replica ID."
        )

    start_monitor = _as_bool(_text(context, "start_monitor"), "start_monitor")
    start_supervisor = _as_bool(
        _text(context, "start_supervisor"), "start_supervisor"
    )
    start_client = _as_bool(_text(context, "start_client"), "start_client")

    decision_timeout_sec = _as_float(
        _text(context, "decision_timeout_sec"), "decision_timeout_sec"
    )
    heartbeat_period_sec = _as_float(
        _text(context, "heartbeat_period_sec"), "heartbeat_period_sec"
    )
    allow_confirmed_release = _as_bool(
        _text(context, "allow_confirmed_release"), "allow_confirmed_release"
    )

    emergency_stop = _as_bool(
        _text(context, "emergency_stop"), "emergency_stop"
    )
    request_id = _text(context, "request_id")
    client_start_delay_sec = _as_float(
        _text(context, "client_start_delay_sec"), "client_start_delay_sec"
    )
    client_publish_delay_sec = _as_float(
        _text(context, "client_publish_delay_sec"), "client_publish_delay_sec"
    )
    client_decision_timeout_sec = _as_float(
        _text(context, "client_decision_timeout_sec"),
        "client_decision_timeout_sec",
    )

    if len(fault_map) > max_faulty:
        fault_bound_message = (
            "WARNING: configured Byzantine replica count exceeds the assumed "
            f"fault bound: actual={len(fault_map)}, f={max_faulty}. "
            "A decision is not guaranteed."
        )
    else:
        fault_bound_message = (
            f"Fault configuration is within the PBFT bound: "
            f"actual={len(fault_map)}, f={max_faulty}."
        )

    actions = [
        LogInfo(
            msg=(
                "CUSTOM PBFT DEMO: "
                f"n={replica_count}, f={max_faulty}, view={initial_view}, "
                f"primary={primary_id}, prepare_threshold={2 * max_faulty}, "
                f"commit_threshold={2 * max_faulty + 1}, faults={fault_map}, "
                f"progress_timeout_nodes={sorted(progress_nodes)}"
            )
        ),
        LogInfo(msg=fault_bound_message),
    ]

    for node_id in range(replica_count):
        behavior = fault_map.get(node_id, "none")
        parameters = {
            "node_id": node_id,
            "primary_id": primary_id,
            "current_view": initial_view,
            "replica_count": replica_count,
            "max_faulty": max_faulty,
            "is_byzantine": behavior != "none",
            "byzantine_behavior": behavior,
            "duplicate_message_count": duplicate_message_count,
            "prepare_delay_sec": prepare_delay_sec,
            "commit_delay_sec": commit_delay_sec,
            "enable_progress_timeout": node_id in progress_nodes,
            "progress_timeout_sec": progress_timeout_sec,
            "new_view_pre_prepare_delay_sec": new_view_pre_prepare_delay_sec,
            "status_publish_period_sec": status_publish_period_sec,
            "protocol_qos_depth": protocol_qos_depth,
            "status_qos_depth": status_qos_depth,
        }

        if node_id == manual_node:
            parameters["manual_view_change_target"] = manual_target
            parameters["manual_view_change_delay_sec"] = manual_delay

        actions.append(
            Node(
                package=PACKAGE_NAME,
                executable="pbft_replica",
                name=f"pbft_node_{node_id}",
                output="screen",
                emulate_tty=True,
                parameters=[parameters],
            )
        )

    if start_monitor:
        actions.append(
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

    if start_supervisor:
        actions.append(
            Node(
                package=PACKAGE_NAME,
                executable="safety_supervisor",
                name="safety_supervisor",
                output="screen",
                emulate_tty=True,
                parameters=[
                    {
                        "replica_count": replica_count,
                        "max_faulty": max_faulty,
                        "decision_timeout_sec": decision_timeout_sec,
                        "allow_confirmed_release": allow_confirmed_release,
                        "heartbeat_period_sec": heartbeat_period_sec,
                    }
                ],
            )
        )

    if start_client:
        client_node = Node(
            package=PACKAGE_NAME,
            executable="client_node",
            name="client_node",
            output="screen",
            emulate_tty=True,
            parameters=[
                {
                    "primary_id": primary_id,
                    "current_view": initial_view,
                    "emergency_stop": emergency_stop,
                    "request_id": request_id,
                    "publish_delay_sec": client_publish_delay_sec,
                    "decision_timeout_sec": client_decision_timeout_sec,
                }
            ],
        )
        actions.append(
            TimerAction(period=client_start_delay_sec, actions=[client_node])
        )

    return actions


def generate_launch_description() -> LaunchDescription:
    """Create the configurable PBFT demonstration launch description."""
    arguments = [
        DeclareLaunchArgument("replica_count", default_value="4"),
        DeclareLaunchArgument("max_faulty", default_value="1"),
        DeclareLaunchArgument("initial_view", default_value="0"),
        DeclareLaunchArgument(
            "faulty_nodes",
            default_value="none",
            description="Comma-separated Byzantine replica IDs, for example: 3 or 5,6.",
        ),
        DeclareLaunchArgument(
            "faulty_behaviors",
            default_value="none",
            description=(
                "One behavior for all faulty nodes or a comma-separated behavior "
                "per node."
            ),
        ),
        DeclareLaunchArgument(
            "progress_timeout_nodes",
            default_value="none",
            description="none, all, backups, correct, faulty, or comma-separated IDs.",
        ),
        DeclareLaunchArgument("progress_timeout_sec", default_value="3.0"),
        DeclareLaunchArgument("prepare_delay_sec", default_value="4.0"),
        DeclareLaunchArgument("commit_delay_sec", default_value="4.0"),
        DeclareLaunchArgument("duplicate_message_count", default_value="3"),
        DeclareLaunchArgument(
            "new_view_pre_prepare_delay_sec", default_value="0.5"
        ),
        DeclareLaunchArgument("status_publish_period_sec", default_value="1.0"),
        DeclareLaunchArgument("protocol_qos_depth", default_value="20"),
        DeclareLaunchArgument("status_qos_depth", default_value="1"),
        DeclareLaunchArgument("manual_view_change_node", default_value="none"),
        DeclareLaunchArgument("manual_view_change_target", default_value="1"),
        DeclareLaunchArgument("manual_view_change_delay_sec", default_value="2.0"),
        DeclareLaunchArgument("start_monitor", default_value="true"),
        DeclareLaunchArgument("start_supervisor", default_value="true"),
        DeclareLaunchArgument("start_client", default_value="true"),
        DeclareLaunchArgument("decision_timeout_sec", default_value="10.0"),
        DeclareLaunchArgument("heartbeat_period_sec", default_value="0.5"),
        DeclareLaunchArgument("allow_confirmed_release", default_value="false"),
        DeclareLaunchArgument("emergency_stop", default_value="true"),
        DeclareLaunchArgument("request_id", default_value="custom-demo"),
        DeclareLaunchArgument("client_start_delay_sec", default_value="1.0"),
        DeclareLaunchArgument("client_publish_delay_sec", default_value="0.8"),
        DeclareLaunchArgument("client_decision_timeout_sec", default_value="20.0"),
    ]

    return LaunchDescription(arguments + [OpaqueFunction(function=_launch_setup)])
