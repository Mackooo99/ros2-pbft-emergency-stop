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

from pbft_emergency_stop_interfaces.msg import (
    PBFTDecision,
    PBFTMessage,
    ReplicaStatus,
)


def create_status_qos() -> QoSProfile:
    """Create QoS compatible with replica status publishers."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=1,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )

def create_protocol_qos() -> QoSProfile:
    """Create QoS for PBFT protocol observation."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=50,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.VOLATILE,
    )


def create_decision_qos() -> QoSProfile:
    """Create QoS for confirmed external PBFT decisions."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=20,
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
        self._validate_configuration()

        self.commit_threshold = 2 * self.max_faulty + 1

        self.latest_status: dict[int, ReplicaStatus] = {}
        self.latest_snapshot: dict[int, tuple] = {}

        self.committed_observations: dict[
            tuple,
            set[int],
        ] = defaultdict(set)

        self.committed_payloads_by_instance = defaultdict(
            lambda: defaultdict(set)
        )

        self.reported_consensus: set[tuple] = set()
        self.reported_agreement_violations: set[tuple] = set()

        self.protocol_observations = defaultdict(lambda: defaultdict(set))
        self.reported_equivocations: set[tuple] = set()

        self.status_subscription = self.create_subscription(
            ReplicaStatus,
            "/pbft/status",
            self.status_callback,
            create_status_qos(),
        )

        self.prepare_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/prepare",
            self.protocol_message_callback,
            create_protocol_qos(),
        )

        self.commit_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/commit",
            self.protocol_message_callback,
            create_protocol_qos(),
        )

        # The monitor does not participate in PBFT. It publishes one
        # external decision only after observing a confirmed quorum of
        # matching COMMITTED states from correct simulator replicas.
        self.decision_publisher = self.create_publisher(
            PBFTDecision,
            "/pbft/decision",
            create_decision_qos(),
        )

        self.get_logger().info(
            "PBFT monitor started: "
            f"n={self.replica_count}, "
            f"f={self.max_faulty}, "
            f"consensus_threshold={self.commit_threshold}"
        )



    def _validate_configuration(self) -> None:
        """Validate the PBFT configuration observed by the monitor."""
        if self.max_faulty < 0:
            raise ValueError(
                "max_faulty must be non-negative."
            )

        expected_replica_count = 3 * self.max_faulty + 1

        if self.replica_count != expected_replica_count:
            raise ValueError(
                "Invalid PBFT monitor configuration: "
                f"n={self.replica_count}, "
                f"f={self.max_faulty}. "
                "This simulator currently requires "
                f"n = 3f + 1 = {expected_replica_count}."
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

        self._record_committed_status(status)
        self._check_agreement()
        self._check_consensus()


    def _record_committed_status(
        self,
        status: ReplicaStatus,
    ) -> None:
        """Persist one correct replica's committed observation."""
        if not status.committed or status.is_byzantine:
            return

        decision = (
            status.view,
            status.sequence_number,
            status.request_id,
            status.request_digest,
            status.emergency_stop,
        )

        self.committed_observations[decision].add(
            status.node_id
        )

        instance_key = (
            status.view,
            status.sequence_number,
        )

        payload = (
            status.request_id,
            status.request_digest,
            status.emergency_stop,
        )

        self.committed_payloads_by_instance[
            instance_key
        ][payload].add(status.node_id)


    def protocol_message_callback(
        self,
        message: PBFTMessage,
    ) -> None:
        """Detect conflicting messages from the same sender."""
        if message.message_type not in (
            PBFTMessage.PREPARE,
            PBFTMessage.COMMIT,
        ):
            return

        observation_key = (
            message.message_type,
            message.sender_id,
            message.view,
            message.sequence_number,
        )

        # recipient_id is intentionally excluded.
        # Equivocation means different payloads, not merely
        # delivery of the same payload to different recipients.
        payload = (
            message.request_id,
            message.request_digest,
            message.emergency_stop,
        )

        self.protocol_observations[
            observation_key
        ][payload].add(message.recipient_id)

        variants = self.protocol_observations[
            observation_key
        ]

        if len(variants) <= 1:
            return

        if observation_key in self.reported_equivocations:
            return

        self.reported_equivocations.add(observation_key)

        phase_name = (
            "PREPARE"
            if message.message_type == PBFTMessage.PREPARE
            else "COMMIT"
        )

        variant_descriptions = []

        for variant_payload, recipients in variants.items():
            request_id, digest, emergency_stop = (
                variant_payload
            )

            variant_descriptions.append(
                {
                    "request_id": request_id,
                    "digest": f"{digest[:12]}...",
                    "emergency_stop": emergency_stop,
                    "recipients": sorted(recipients),
                }
            )

        self.get_logger().error(
            "EQUIVOCATION DETECTED: "
            f"sender={message.sender_id}, "
            f"phase={phase_name}, "
            f"view={message.view}, "
            f"sequence={message.sequence_number}, "
            f"variants={variant_descriptions}"
        )


    def _check_agreement(self) -> None:
        """Detect conflicting values for the same PBFT instance."""
        for (
            instance_key,
            variants,
        ) in self.committed_payloads_by_instance.items():
            if len(variants) <= 1:
                continue

            if instance_key in self.reported_agreement_violations:
                continue

            self.reported_agreement_violations.add(instance_key)

            view, sequence_number = instance_key

            self.get_logger().error(
                "AGREEMENT VIOLATION: correct replicas committed "
                "conflicting values for the same PBFT instance: "
                f"view={view}, sequence={sequence_number}."
            )

    def _check_consensus(self) -> None:
        """Report when enough replicas committed the same decision."""
        for (
            decision,
            node_ids,
        ) in self.committed_observations.items():
            if len(node_ids) < self.commit_threshold:
                continue

            if decision in self.reported_consensus:
                continue

            self.reported_consensus.add(decision)

            view, sequence, request_id, digest, emergency_stop = decision

            decision_message = PBFTDecision()
            decision_message.stamp = self.get_clock().now().to_msg()
            decision_message.view = view
            decision_message.sequence_number = sequence
            decision_message.request_id = request_id
            decision_message.request_digest = digest
            decision_message.emergency_stop = emergency_stop
            decision_message.committed = True
            decision_message.confirmation_count = len(node_ids)
            decision_message.required_confirmations = (
                self.commit_threshold
            )
            decision_message.confirming_replicas = sorted(node_ids)

            self.decision_publisher.publish(decision_message)

            self.get_logger().info(
                "Published confirmed decision on /pbft/decision: "
                f"request_id={request_id}, "
                f"confirmations={sorted(node_ids)}"
            )

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
                f"correctly_committed_replicas={sorted(node_ids)}"
            )

            byzantine_replicas = [
                status.node_id
                for status in self.latest_status.values()
                if status.is_byzantine
            ]

            self.get_logger().info(
                f"byzantine_replicas={sorted(byzantine_replicas)}"
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
