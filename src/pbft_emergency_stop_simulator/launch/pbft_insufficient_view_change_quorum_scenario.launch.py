"""Verify that two VIEW-CHANGE messages cannot form NEW-VIEW."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Launch a faulty primary with only two timeout participants."""
    nodes = []

    for node_id in range(4):
        is_faulty_primary = node_id == 0

        # Only nodes 1 and 2 initiate VIEW-CHANGE.
        timeout_enabled = node_id in {1, 2}

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
            "enable_progress_timeout": timeout_enabled,
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
                    "request_id": (
                        "insufficient-view-change-quorum-test"
                    ),
                    "emergency_stop": True,
                    "publish_delay_sec": 1.0,
                }
            ],
        )
    )

    return LaunchDescription(nodes)
