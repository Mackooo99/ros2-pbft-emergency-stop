"""Build NEW-VIEW from three PREPARED certificates."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Create PREPARED state, then trigger three VIEW-CHANGE votes."""
    nodes = []

    # Only nodes 0 and 1 publish COMMIT, so no COMMIT quorum exists.
    skip_commit_node_ids = {2, 3}

    for node_id in range(4):
        skips_commit = node_id in skip_commit_node_ids

        parameters = {
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

        if node_id in {1, 2, 3}:
            parameters.update(
                {
                    "manual_view_change_target": 1,
                    "manual_view_change_delay_sec": 5.0,
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
                    "request_id": "prepared-new-view-test",
                    "emergency_stop": True,
                    "publish_delay_sec": 1.0,
                }
            ],
        )
    )

    return LaunchDescription(nodes)
