"""Trigger VIEW-CHANGE when the primary skips PRE-PREPARE."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Launch one faulty primary and three timeout-enabled backups."""
    nodes = []

    for node_id in range(4):
        is_faulty_primary = node_id == 0

        parameters = {
            "node_id": node_id,
            "primary_id": 0,
            "current_view": 0,
            "replica_count": 4,
            "max_faulty": 1,
            "is_byzantine": is_faulty_primary,
            "byzantine_behavior": (
                "skip_pre_prepare"
                if is_faulty_primary
                else "none"
            ),
            "enable_progress_timeout": (
                not is_faulty_primary
            ),
            "progress_timeout_sec": 3.0,
        }

        nodes.append(
            Node(
                package="pbft_emergency_stop_simulator",
                executable="pbft_replica",
                name=f"pbft_node_{node_id}",
                output="screen",
                emulate_tty=True,
                parameters=[parameters],
            )
        )

    nodes.append(
        Node(
            package="pbft_emergency_stop_simulator",
            executable="pbft_monitor",
            name="pbft_monitor",
            output="screen",
            emulate_tty=True,
            parameters=[
                {
                    "replica_count": 4,
                    "max_faulty": 1,
                }
            ],
        )
    )

    nodes.append(
        Node(
            package="pbft_emergency_stop_simulator",
            executable="safety_supervisor",
            name="safety_supervisor",
            output="screen",
            emulate_tty=True,
            parameters=[
                {
                    "replica_count": 4,
                    "max_faulty": 1,
                    "decision_timeout_sec": 8.0,
                    "allow_confirmed_release": False,
                    "heartbeat_period_sec": 0.5,
                }
            ],
        )
    )

    nodes.append(
        Node(
            package="pbft_emergency_stop_simulator",
            executable="client_node",
            name="pbft_client",
            output="screen",
            emulate_tty=True,
            parameters=[
                {
                    "primary_id": 0,
                    "current_view": 0,
                    "request_id": "primary-timeout-test",
                    "emergency_stop": True,
                    "publish_delay_sec": 1.0,
                }
            ],
        )
    )

    return LaunchDescription(nodes)
