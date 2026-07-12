"""Launch a PBFT scenario in which a COMMIT quorum is unavailable."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Launch two correct replicas and two replicas that skip COMMIT."""
    nodes = []

    # This scenario intentionally exceeds the configured fault bound f=1.
    # Replicas 2 and 3 participate in PREPARE but do not publish COMMIT.
    # Therefore, only two distinct COMMIT senders remain available.
    skip_commit_node_ids = {2, 3}

    for node_id in range(4):
        skips_commit = node_id in skip_commit_node_ids

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
                        "is_byzantine": skips_commit,
                        "byzantine_behavior": (
                            "skip_commit"
                            if skips_commit
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