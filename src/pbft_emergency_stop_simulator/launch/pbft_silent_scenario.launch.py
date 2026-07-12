"""Launch PBFT with replica 3 in silent Byzantine mode."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Launch three correct replicas, one silent replica and monitor."""
    nodes = []

    for node_id in range(4):
        is_silent_replica = node_id == 3

        nodes.append(
            Node(
                package="pbft_emergency_stop_simulator",
                executable="pbft_replica",
                name=f"pbft_node_{node_id}",
                output="screen",
                emulate_tty=True,
                parameters=[
                    {
                        "node_id": node_id,
                        "primary_id": 0,
                        "current_view": 0,
                        "replica_count": 4,
                        "max_faulty": 1,
                        "is_byzantine": is_silent_replica,
                        "byzantine_behavior": (
                            "silent"
                            if is_silent_replica
                            else "none"
                        ),
                    }
                ],
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

    return LaunchDescription(nodes)
