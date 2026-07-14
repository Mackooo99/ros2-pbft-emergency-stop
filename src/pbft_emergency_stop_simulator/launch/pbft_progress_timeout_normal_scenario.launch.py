"""Verify that normal PBFT progress cancels the timeout."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Launch a correct PBFT system with progress timers enabled."""
    nodes = []

    for node_id in range(4):
        parameters = {
            "node_id": node_id,
            "primary_id": 0,
            "current_view": 0,
            "replica_count": 4,
            "max_faulty": 1,
            "is_byzantine": False,
            "byzantine_behavior": "none",
            "enable_progress_timeout": True,
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
            executable="client_node",
            name="pbft_client",
            output="screen",
            emulate_tty=True,
            parameters=[
                {
                    "primary_id": 0,
                    "current_view": 0,
                    "request_id": "normal-timeout-test",
                    "emergency_stop": True,
                    "publish_delay_sec": 1.0,
                }
            ],
        )
    )

    return LaunchDescription(nodes)
