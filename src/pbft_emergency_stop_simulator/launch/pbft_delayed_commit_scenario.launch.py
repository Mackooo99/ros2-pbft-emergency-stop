"""Launch PBFT with replica 3 using delayed_commit behavior."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Launch the isolated delayed_commit scenario."""
    nodes = []

    for node_id in range(4):
        is_delayed_replica = node_id == 3

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
                        "is_byzantine": is_delayed_replica,
                        "byzantine_behavior": (
                            "delayed_commit"
                            if is_delayed_replica
                            else "none"
                        ),
                        "commit_delay_sec": 4.0,
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
