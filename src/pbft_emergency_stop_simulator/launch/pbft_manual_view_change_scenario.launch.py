"""Launch one controlled VIEW-CHANGE without a prepared certificate."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Launch node 2 with a controlled VIEW-CHANGE trigger."""
    nodes = []

    for node_id in range(4):
        parameters = {
            "node_id": node_id,
            "primary_id": 0,
            "current_view": 0,
            "replica_count": 4,
            "max_faulty": 1,
            "is_byzantine": False,
        }

        if node_id == 2:
            parameters.update(
                {
                    "manual_view_change_target": 1,
                    "manual_view_change_delay_sec": 3.0,
                }
            )

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

    return LaunchDescription(nodes)
