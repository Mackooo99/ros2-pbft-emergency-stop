"""Launch the four PBFT replicas."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Create the PBFT replica launch description."""
    replica_nodes = []

    for node_id in range(4):
        replica_nodes.append(
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
                    }
                ],
            )
        )

    return LaunchDescription(replica_nodes)
