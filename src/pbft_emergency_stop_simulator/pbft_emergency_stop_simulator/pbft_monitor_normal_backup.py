"""Passive monitoring node for the PBFT simulator."""

from collections import defaultdict

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    QoSProfile,
    ReliabilityPolicy,
)

from pbft_emergency_stop_interfaces.msg import ReplicaStatus


def create_status_qos() -> QoSProfile:
    """Create QoS compatible with replica status publishers."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=1,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )


class PBFTMonitor(Node):
    """Observe replica states without participating in consensus."""

    def __init__(self) -> None:
        super().__init__("pbft_monitor")

        self.declare_parameter("replica_count", 4)
        self.declare_parameter("max_faulty", 1)

        self.replica_count = int(
            self.get_parameter("replica_count").value
        )
        self.max_faulty = int(
            self.get_parameter("max_faulty").value
        )
        self.commit_threshold = 2 * self.max_faulty + 1

        self.latest_status: dict[int, ReplicaStatus] = {}
        self.latest_snapshot: dict[int, tuple] = {}
        self.reported_consensus: set[tuple] = set()

        self.status_subscription = self.create_subscription(
            ReplicaStatus,
            "/pbft/status",
            self.status_callback,
            create_status_qos(),
        )

        self.get_logger().info(
            "PBFT monitor started: "
            f"n={self.replica_count}, "
            f"f={self.max_faulty}, "
            f"consensus_threshold={self.commit_threshold}"
        )

    def status_callback(self, status: ReplicaStatus) -> None:
        """Store and evaluate one replica status update."""
        if not 0 <= status.node_id < self.replica_count:
            self.get_logger().warning(
                f"Ignored status with invalid node_id={status.node_id}."
            )
            return

        snapshot = (
            status.view,
            status.sequence_number,
            status.request_id,
            status.request_digest,
            status.phase,
            status.prepare_count,
            status.commit_count,
            status.prepared,
            status.committed,
            status.emergency_stop,
            status.is_byzantine,
        )

        previous_snapshot = self.latest_snapshot.get(status.node_id)

        self.latest_status[status.node_id] = status
        self.latest_snapshot[status.node_id] = snapshot

        if snapshot != previous_snapshot:
            self.get_logger().info(
                f"REPLICA {status.node_id}: "
                f"phase={status.phase}, "
                f"sequence={status.sequence_number}, "
                f"prepare={status.prepare_count}, "
                f"commit={status.commit_count}, "
                f"prepared={status.prepared}, "
                f"committed={status.committed}, "
                f"emergency_stop={status.emergency_stop}, "
                f"byzantine={status.is_byzantine}"
            )

        self._check_agreement()
        self._check_consensus()

    def _check_agreement(self) -> None:
        """Detect conflicting committed values among replicas."""
        committed_statuses = [
            status
            for status in self.latest_status.values()
            if status.committed
        ]

        committed_values = {
            (
                status.view,
                status.sequence_number,
                status.request_id,
                status.request_digest,
                status.emergency_stop,
            )
            for status in committed_statuses
        }

        if len(committed_values) > 1:
            self.get_logger().error(
                "AGREEMENT VIOLATION: correct replicas appear to "
                "have committed conflicting PBFT values."
            )

    def _check_consensus(self) -> None:
        """Report when enough replicas committed the same decision."""
        groups: dict[tuple, set[int]] = defaultdict(set)

        for status in self.latest_status.values():
            if not status.committed:
                continue

            decision = (
                status.view,
                status.sequence_number,
                status.request_id,
                status.request_digest,
                status.emergency_stop,
            )

            groups[decision].add(status.node_id)

        for decision, node_ids in groups.items():
            if len(node_ids) < self.commit_threshold:
                continue

            if decision in self.reported_consensus:
                continue

            self.reported_consensus.add(decision)

            view, sequence, request_id, digest, emergency_stop = decision

            self.get_logger().info(
                "=================================================="
            )
            self.get_logger().info(
                "PBFT CONSENSUS CONFIRMED"
            )
            self.get_logger().info(
                f"view={view}, sequence={sequence}"
            )
            self.get_logger().info(
                f"request_id={request_id}"
            )
            self.get_logger().info(
                f"digest={digest[:12]}..."
            )
            self.get_logger().info(
                f"committed_replicas={sorted(node_ids)}"
            )
            self.get_logger().info(
                f"emergency_stop={emergency_stop}"
            )
            self.get_logger().info(
                "=================================================="
            )


def main(args=None) -> None:
    """Run the PBFT monitoring node."""
    rclpy.init(args=args)

    node = PBFTMonitor()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
