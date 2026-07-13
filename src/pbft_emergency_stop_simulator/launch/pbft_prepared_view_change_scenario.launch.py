"""Launch VIEW-CHANGE carrying a local PREPARED certificate."""

from launch import LaunchDescription
from launch_ros.actions import Node


def generate_launch_description() -> LaunchDescription:
    """Create PREPARED state without allowing a COMMIT quorum."""
    nodes = []

    # Replicas 2 and 3 do not publish COMMIT.
    # This intentionally exceeds f=1 for this negative liveness test.
    # PREPARED can be reached, but only two COMMIT senders remain.
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

        # Node 1 will later be the primary for view 1.
        if node_id == 1:
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
                    "request_id": "prepared-certificate-test",
                    "emergency_stop": True,
                    "publish_delay_sec": 1.0,
                }
            ],
        )
    )

    return LaunchDescription(nodes)
