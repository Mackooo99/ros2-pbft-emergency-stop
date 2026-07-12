"""PBFT replica implementation through the COMMIT phase."""

from collections import defaultdict
from dataclasses import dataclass

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    QoSProfile,
    ReliabilityPolicy,
)

from pbft_emergency_stop_interfaces.msg import (
    PBFTMessage,
    ReplicaStatus,
)

from .protocol import compute_request_digest


MessageKey = tuple[int, int]


@dataclass(frozen=True)
class PBFTInstance:
    """Locally stored PBFT request data."""

    request_id: str
    request_digest: str
    emergency_stop: bool


def create_pbft_qos() -> QoSProfile:
    """Create the QoS profile used for PBFT protocol messages."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=20,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.VOLATILE,
    )
    
def create_status_qos() -> QoSProfile:
    """Create QoS for the latest replica status."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=1,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )


class PBFTReplica(Node):
    """PBFT replica supporting REQUEST through COMMIT."""

    def __init__(self) -> None:
        super().__init__("pbft_replica")

        self.declare_parameter("node_id", 0)
        self.declare_parameter("primary_id", 0)
        self.declare_parameter("current_view", 0)
        self.declare_parameter("replica_count", 4)
        self.declare_parameter("max_faulty", 1)
        self.declare_parameter("is_byzantine", False)

        self.node_id = int(
            self.get_parameter("node_id").value
        )
        self.primary_id = int(
            self.get_parameter("primary_id").value
        )
        self.current_view = int(
            self.get_parameter("current_view").value
        )
        self.replica_count = int(
            self.get_parameter("replica_count").value
        )
        self.max_faulty = int(
            self.get_parameter("max_faulty").value
        )
        
        self.is_byzantine = bool(
	    self.get_parameter("is_byzantine").value
	)

        self._validate_configuration()

        self.prepare_threshold = 2 * self.max_faulty
        self.commit_threshold = 2 * self.max_faulty + 1

        self.next_sequence_number = 1

        # Replicated application state.
        self.emergency_stop = False
        
        self.phase = "IDLE"
        self.current_key: MessageKey | None = None
        self.status_detail = "Replica initialized."

        # REQUEST bookkeeping.
        self.processed_request_ids: set[str] = set()

        # Local PBFT instances indexed by (view, sequence_number).
        self.instances: dict[MessageKey, PBFTInstance] = {}

        # PREPARE state.
        self.prepare_senders: dict[
            MessageKey, set[int]
        ] = defaultdict(set)

        self.pending_prepares: dict[
            MessageKey, dict[int, PBFTMessage]
        ] = defaultdict(dict)

        self.prepare_sent: set[MessageKey] = set()
        self.prepared_instances: set[MessageKey] = set()

        # COMMIT state.
        self.commit_senders: dict[
            MessageKey, set[int]
        ] = defaultdict(set)

        self.pending_commits: dict[
            MessageKey, dict[int, PBFTMessage]
        ] = defaultdict(dict)

        self.commit_sent: set[MessageKey] = set()
        self.committed_instances: set[MessageKey] = set()

        # Publishers.
        self.pre_prepare_publisher = self.create_publisher(
            PBFTMessage,
            "/pbft/pre_prepare",
            create_pbft_qos(),
        )

        self.prepare_publisher = self.create_publisher(
            PBFTMessage,
            "/pbft/prepare",
            create_pbft_qos(),
        )

        self.commit_publisher = self.create_publisher(
            PBFTMessage,
            "/pbft/commit",
            create_pbft_qos(),
        )
        
        self.status_publisher = self.create_publisher(
	    ReplicaStatus,
	    "/pbft/status",
	    create_status_qos(),
	)

        # Subscriptions.
        self.request_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/request",
            self.request_callback,
            create_pbft_qos(),
        )

        self.pre_prepare_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/pre_prepare",
            self.pre_prepare_callback,
            create_pbft_qos(),
        )

        self.prepare_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/prepare",
            self.prepare_callback,
            create_pbft_qos(),
        )

        self.commit_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/commit",
            self.commit_callback,
            create_pbft_qos(),
        )
        
        self.status_timer = self.create_timer(
	    1.0,
	    self._publish_status,
	)

        role = (
            "PRIMARY"
            if self.node_id == self.primary_id
            else "BACKUP"
        )

        self.get_logger().info(
            f"Replica initialized: node_id={self.node_id}, "
            f"role={role}, "
            f"view={self.current_view}, "
            f"primary_id={self.primary_id}, "
            f"n={self.replica_count}, "
            f"f={self.max_faulty}, "
            f"prepare_threshold={self.prepare_threshold}, "
            f"commit_threshold={self.commit_threshold}, "
            f"emergency_stop={self.emergency_stop}"
        )
        
        self._publish_status("Replica initialized.")
        

    def _publish_status(self, detail: str = "") -> None:
        """Publish the current local state of this replica."""
        if detail:
            self.status_detail = detail

        status = ReplicaStatus()

        status.stamp = self.get_clock().now().to_msg()
        status.node_id = self.node_id
        status.view = self.current_view

        if self.current_key is None:
            status.sequence_number = 0
            status.request_id = ""
            status.request_digest = ""

            prepare_count = 0
            commit_count = 0
            prepared = False
            committed = False
        else:
            key = self.current_key
            instance = self.instances.get(key)

            status.sequence_number = key[1]

            if instance is None:
                status.request_id = ""
                status.request_digest = ""
            else:
                status.request_id = instance.request_id
                status.request_digest = instance.request_digest

            prepare_count = len(
                self.prepare_senders.get(key, set())
            )
            commit_count = len(
                self.commit_senders.get(key, set())
            )
            prepared = key in self.prepared_instances
            committed = key in self.committed_instances

        status.phase = self.phase
        status.prepare_count = prepare_count
        status.commit_count = commit_count
        status.prepared = prepared
        status.committed = committed
        status.emergency_stop = self.emergency_stop
        status.is_byzantine = self.is_byzantine
        status.detail = self.status_detail

        self.status_publisher.publish(status)

    def _validate_configuration(self) -> None:
        """Validate replica identity and PBFT parameters."""
        minimum_replica_count = 3 * self.max_faulty + 1

        if self.replica_count < minimum_replica_count:
            raise ValueError(
                "Invalid PBFT configuration: "
                f"n={self.replica_count}, "
                f"f={self.max_faulty}. "
                f"Required: n >= {minimum_replica_count}."
            )

        if not 0 <= self.node_id < self.replica_count:
            raise ValueError(
                f"node_id={self.node_id} is outside the valid range."
            )

        if not 0 <= self.primary_id < self.replica_count:
            raise ValueError(
                f"primary_id={self.primary_id} is outside the valid range."
            )

    def request_callback(self, message: PBFTMessage) -> None:
        """Validate a client REQUEST and publish PRE-PREPARE."""
        if message.message_type != PBFTMessage.REQUEST:
            self.get_logger().warning(
                "Rejected message on /pbft/request: "
                f"message_type={message.message_type}"
            )
            return

        # Only the current primary processes client requests.
        if self.node_id != self.primary_id:
            return

        if message.sender_id != -1:
            self.get_logger().warning(
                "Rejected REQUEST with an invalid client sender_id: "
                f"{message.sender_id}"
            )
            return

        if message.recipient_id not in (-1, self.node_id):
            self.get_logger().warning(
                "Rejected REQUEST intended for another replica: "
                f"recipient_id={message.recipient_id}"
            )
            return

        if message.view != self.current_view:
            self.get_logger().warning(
                "Rejected REQUEST with an invalid view: "
                f"received={message.view}, "
                f"expected={self.current_view}"
            )
            return

        if not message.request_id:
            self.get_logger().warning(
                "Rejected REQUEST with an empty request_id."
            )
            return

        if message.request_id in self.processed_request_ids:
            self.get_logger().warning(
                "Duplicate REQUEST ignored: "
                f"request_id={message.request_id}"
            )
            return

        expected_digest = compute_request_digest(
            message.request_id,
            message.emergency_stop,
        )

        if message.request_digest != expected_digest:
            self.get_logger().warning(
                "Rejected REQUEST because its digest is invalid: "
                f"request_id={message.request_id}"
            )
            return

        if not message.emergency_stop:
            self.get_logger().warning(
                "Rejected REQUEST because this simulator currently "
                "supports only emergency_stop=true."
            )
            return

        sequence_number = self.next_sequence_number
        self.next_sequence_number += 1

        key = (self.current_view, sequence_number)

        instance = PBFTInstance(
            request_id=message.request_id,
            request_digest=message.request_digest,
            emergency_stop=message.emergency_stop,
        )

        self.instances[key] = instance
        self.processed_request_ids.add(message.request_id)
        
        self.current_key = key
        self.phase = "PRE_PREPARED"
        self._publish_status(
            "Primary accepted REQUEST and assigned a sequence number."
        )

        self.get_logger().info(
            "Accepted valid REQUEST: "
            f"request_id={message.request_id}, "
            f"assigned_sequence={sequence_number}, "
            f"digest={message.request_digest[:12]}..."
        )

        pre_prepare = PBFTMessage()

        pre_prepare.stamp = self.get_clock().now().to_msg()
        pre_prepare.message_type = PBFTMessage.PRE_PREPARE
        pre_prepare.sender_id = self.node_id
        pre_prepare.recipient_id = -1
        pre_prepare.view = self.current_view
        pre_prepare.sequence_number = sequence_number
        pre_prepare.request_id = message.request_id
        pre_prepare.request_digest = message.request_digest
        pre_prepare.emergency_stop = message.emergency_stop

        self.pre_prepare_publisher.publish(pre_prepare)

        self.get_logger().info(
            "Published PRE-PREPARE: "
            f"view={pre_prepare.view}, "
            f"sequence={pre_prepare.sequence_number}, "
            f"request_id={pre_prepare.request_id}, "
            f"digest={pre_prepare.request_digest[:12]}..."
        )

    def pre_prepare_callback(
        self,
        message: PBFTMessage,
    ) -> None:
        """Validate PRE-PREPARE and send PREPARE as a backup."""
        if message.message_type != PBFTMessage.PRE_PREPARE:
            self.get_logger().warning(
                "Rejected message on /pbft/pre_prepare: "
                f"message_type={message.message_type}"
            )
            return

        if message.sender_id != self.primary_id:
            self.get_logger().warning(
                "Rejected PRE-PREPARE not sent by the primary: "
                f"sender_id={message.sender_id}"
            )
            return

        if message.recipient_id not in (-1, self.node_id):
            return

        if message.view != self.current_view:
            self.get_logger().warning(
                "Rejected PRE-PREPARE with an invalid view: "
                f"received={message.view}, "
                f"expected={self.current_view}"
            )
            return

        if message.sequence_number == 0:
            self.get_logger().warning(
                "Rejected PRE-PREPARE with sequence_number=0."
            )
            return

        if not message.request_id:
            self.get_logger().warning(
                "Rejected PRE-PREPARE with an empty request_id."
            )
            return

        expected_digest = compute_request_digest(
            message.request_id,
            message.emergency_stop,
        )

        if message.request_digest != expected_digest:
            self.get_logger().warning(
                "Rejected PRE-PREPARE because its digest is invalid: "
                f"sequence={message.sequence_number}"
            )
            return

        if not message.emergency_stop:
            self.get_logger().warning(
                "Rejected PRE-PREPARE with emergency_stop=false."
            )
            return

        key = (message.view, message.sequence_number)

        new_instance = PBFTInstance(
            request_id=message.request_id,
            request_digest=message.request_digest,
            emergency_stop=message.emergency_stop,
        )

        existing_instance = self.instances.get(key)

        if (
            existing_instance is not None
            and existing_instance != new_instance
        ):
            self.get_logger().error(
                "Conflicting PRE-PREPARE detected for the same "
                f"(view, sequence)=({message.view}, "
                f"{message.sequence_number})."
            )
            return

        if existing_instance is None:
            self.instances[key] = new_instance

            self.get_logger().info(
                "Accepted PRE-PREPARE: "
                f"view={message.view}, "
                f"sequence={message.sequence_number}, "
                f"request_id={message.request_id}, "
                f"digest={message.request_digest[:12]}..."
            )
            
        self.current_key = key

        if key not in self.prepared_instances:
            self.phase = "PRE_PREPARED"

        self._publish_status(
            "Valid PRE-PREPARE accepted."
        )

        # Process messages which may have arrived before PRE-PREPARE.
        self._process_pending_prepares(key)
        self._process_pending_commits(key)

        # The primary proposes the request but does not send PREPARE
        # in this simplified PBFT model.
        if self.node_id != self.primary_id:
            self._send_prepare(key)

    def _send_prepare(self, key: MessageKey) -> None:
        """Publish one PREPARE for an accepted PRE-PREPARE."""
        if key in self.prepare_sent:
            return

        instance = self.instances[key]
        view, sequence_number = key

        prepare = PBFTMessage()

        prepare.stamp = self.get_clock().now().to_msg()
        prepare.message_type = PBFTMessage.PREPARE
        prepare.sender_id = self.node_id
        prepare.recipient_id = -1
        prepare.view = view
        prepare.sequence_number = sequence_number
        prepare.request_id = instance.request_id
        prepare.request_digest = instance.request_digest
        prepare.emergency_stop = instance.emergency_stop

        self.prepare_sent.add(key)

        # Count the replica's own PREPARE once.
        self._accept_prepare(prepare)

        self.prepare_publisher.publish(prepare)

        self.get_logger().info(
            "Published PREPARE: "
            f"sender={self.node_id}, "
            f"view={view}, "
            f"sequence={sequence_number}, "
            f"digest={instance.request_digest[:12]}..."
        )

    def prepare_callback(self, message: PBFTMessage) -> None:
        """Validate and record an incoming PREPARE."""
        if message.message_type != PBFTMessage.PREPARE:
            self.get_logger().warning(
                "Rejected message on /pbft/prepare: "
                f"message_type={message.message_type}"
            )
            return

        if message.recipient_id not in (-1, self.node_id):
            return

        if message.view != self.current_view:
            self.get_logger().warning(
                "Rejected PREPARE with an invalid view: "
                f"received={message.view}, "
                f"expected={self.current_view}"
            )
            return

        if message.sequence_number == 0:
            self.get_logger().warning(
                "Rejected PREPARE with sequence_number=0."
            )
            return

        if not 0 <= message.sender_id < self.replica_count:
            self.get_logger().warning(
                "Rejected PREPARE with an invalid sender_id: "
                f"{message.sender_id}"
            )
            return

        if message.sender_id == self.primary_id:
            self.get_logger().warning(
                "Rejected PREPARE sent by the primary replica."
            )
            return

        expected_digest = compute_request_digest(
            message.request_id,
            message.emergency_stop,
        )

        if message.request_digest != expected_digest:
            self.get_logger().warning(
                "Rejected PREPARE with an internally invalid digest: "
                f"sender={message.sender_id}"
            )
            return

        if not message.emergency_stop:
            self.get_logger().warning(
                "Rejected PREPARE with emergency_stop=false."
            )
            return

        key = (message.view, message.sequence_number)

        if key not in self.instances:
            self._buffer_early_prepare(key, message)
            return

        self._accept_prepare(message)

    def _buffer_early_prepare(
        self,
        key: MessageKey,
        message: PBFTMessage,
    ) -> None:
        """Store PREPARE received before PRE-PREPARE."""
        existing = self.pending_prepares[key].get(
            message.sender_id
        )

        if existing is not None:
            same_message = (
                existing.request_id == message.request_id
                and existing.request_digest
                == message.request_digest
                and existing.emergency_stop
                == message.emergency_stop
            )

            if not same_message:
                self.get_logger().error(
                    "Conflicting early PREPARE messages received "
                    f"from sender={message.sender_id}."
                )

            return

        self.pending_prepares[key][
            message.sender_id
        ] = message

        self.get_logger().info(
            "Buffered early PREPARE: "
            f"sender={message.sender_id}, "
            f"view={message.view}, "
            f"sequence={message.sequence_number}"
        )

    def _process_pending_prepares(
        self,
        key: MessageKey,
    ) -> None:
        """Process PREPARE messages buffered before PRE-PREPARE."""
        pending = self.pending_prepares.pop(key, {})

        for message in pending.values():
            self._accept_prepare(message)

    def _accept_prepare(self, message: PBFTMessage) -> None:
        """Compare PREPARE with local state and count sender."""
        key = (message.view, message.sequence_number)
        instance = self.instances.get(key)

        if instance is None:
            return

        message_matches_instance = (
            message.request_id == instance.request_id
            and message.request_digest
            == instance.request_digest
            and message.emergency_stop
            == instance.emergency_stop
        )

        if not message_matches_instance:
            self.get_logger().warning(
                "Rejected PREPARE that does not match the local "
                "PRE-PREPARE record: "
                f"sender={message.sender_id}, "
                f"view={message.view}, "
                f"sequence={message.sequence_number}"
            )
            return

        senders = self.prepare_senders[key]

        if message.sender_id in senders:
            return

        senders.add(message.sender_id)
        
        self.current_key = key
        self._publish_status(
            f"Accepted PREPARE from replica {message.sender_id}."
        )

        self.get_logger().info(
            "Accepted PREPARE: "
            f"sender={message.sender_id}, "
            f"view={message.view}, "
            f"sequence={message.sequence_number}, "
            f"prepare_count={len(senders)}, "
            f"threshold={self.prepare_threshold}"
        )

        self._evaluate_prepared(key)

    def _evaluate_prepared(self, key: MessageKey) -> None:
        """Enter PREPARED state when the PREPARE quorum exists."""
        if key in self.prepared_instances:
            return

        prepare_count = len(self.prepare_senders[key])

        if prepare_count < self.prepare_threshold:
            return

        self.prepared_instances.add(key)
        
        self.current_key = key
        self.phase = "PREPARED"
        self._publish_status(
            "PREPARE quorum formed. Replica entered PREPARED state."
        )

        instance = self.instances[key]
        view, sequence_number = key

        self.get_logger().info(
            "PREPARED: "
            f"view={view}, "
            f"sequence={sequence_number}, "
            f"request_id={instance.request_id}, "
            f"prepare_senders="
            f"{sorted(self.prepare_senders[key])}"
        )

        # Every prepared replica broadcasts COMMIT.
        self._send_commit(key)

    def _send_commit(self, key: MessageKey) -> None:
        """Publish one COMMIT after entering PREPARED state."""
        if key in self.commit_sent:
            return

        if key not in self.prepared_instances:
            return

        instance = self.instances[key]
        view, sequence_number = key

        commit = PBFTMessage()

        commit.stamp = self.get_clock().now().to_msg()
        commit.message_type = PBFTMessage.COMMIT
        commit.sender_id = self.node_id
        commit.recipient_id = -1
        commit.view = view
        commit.sequence_number = sequence_number
        commit.request_id = instance.request_id
        commit.request_digest = instance.request_digest
        commit.emergency_stop = instance.emergency_stop

        self.commit_sent.add(key)

        # Count this replica's own COMMIT exactly once.
        self._accept_commit(commit)

        self.commit_publisher.publish(commit)

        self.get_logger().info(
            "Published COMMIT: "
            f"sender={self.node_id}, "
            f"view={view}, "
            f"sequence={sequence_number}, "
            f"digest={instance.request_digest[:12]}..."
        )

    def commit_callback(self, message: PBFTMessage) -> None:
        """Validate and record an incoming COMMIT."""
        if message.message_type != PBFTMessage.COMMIT:
            self.get_logger().warning(
                "Rejected message on /pbft/commit: "
                f"message_type={message.message_type}"
            )
            return

        if message.recipient_id not in (-1, self.node_id):
            return

        if message.view != self.current_view:
            self.get_logger().warning(
                "Rejected COMMIT with an invalid view: "
                f"received={message.view}, "
                f"expected={self.current_view}"
            )
            return

        if message.sequence_number == 0:
            self.get_logger().warning(
                "Rejected COMMIT with sequence_number=0."
            )
            return

        if not 0 <= message.sender_id < self.replica_count:
            self.get_logger().warning(
                "Rejected COMMIT with an invalid sender_id: "
                f"{message.sender_id}"
            )
            return

        if not message.request_id:
            self.get_logger().warning(
                "Rejected COMMIT with an empty request_id."
            )
            return

        expected_digest = compute_request_digest(
            message.request_id,
            message.emergency_stop,
        )

        if message.request_digest != expected_digest:
            self.get_logger().warning(
                "Rejected COMMIT with an internally invalid digest: "
                f"sender={message.sender_id}"
            )
            return

        if not message.emergency_stop:
            self.get_logger().warning(
                "Rejected COMMIT with emergency_stop=false."
            )
            return

        key = (message.view, message.sequence_number)

        if key not in self.instances:
            self._buffer_early_commit(key, message)
            return

        self._accept_commit(message)

    def _buffer_early_commit(
        self,
        key: MessageKey,
        message: PBFTMessage,
    ) -> None:
        """Store COMMIT received before PRE-PREPARE."""
        existing = self.pending_commits[key].get(
            message.sender_id
        )

        if existing is not None:
            same_message = (
                existing.request_id == message.request_id
                and existing.request_digest
                == message.request_digest
                and existing.emergency_stop
                == message.emergency_stop
            )

            if not same_message:
                self.get_logger().error(
                    "Conflicting early COMMIT messages received "
                    f"from sender={message.sender_id}."
                )

            return

        self.pending_commits[key][
            message.sender_id
        ] = message

        self.get_logger().info(
            "Buffered early COMMIT: "
            f"sender={message.sender_id}, "
            f"view={message.view}, "
            f"sequence={message.sequence_number}"
        )

    def _process_pending_commits(
        self,
        key: MessageKey,
    ) -> None:
        """Process COMMIT messages buffered before PRE-PREPARE."""
        pending = self.pending_commits.pop(key, {})

        for message in pending.values():
            self._accept_commit(message)

    def _accept_commit(self, message: PBFTMessage) -> None:
        """Compare COMMIT with local state and count sender."""
        key = (message.view, message.sequence_number)
        instance = self.instances.get(key)

        if instance is None:
            return

        message_matches_instance = (
            message.request_id == instance.request_id
            and message.request_digest
            == instance.request_digest
            and message.emergency_stop
            == instance.emergency_stop
        )

        if not message_matches_instance:
            self.get_logger().warning(
                "Rejected COMMIT that does not match the local "
                "PRE-PREPARE record: "
                f"sender={message.sender_id}, "
                f"view={message.view}, "
                f"sequence={message.sequence_number}"
            )
            return

        senders = self.commit_senders[key]

        if message.sender_id in senders:
            return

        senders.add(message.sender_id)
        
        self.current_key = key
        self._publish_status(
            f"Accepted COMMIT from replica {message.sender_id}."
        )

        self.get_logger().info(
            "Accepted COMMIT: "
            f"sender={message.sender_id}, "
            f"view={message.view}, "
            f"sequence={message.sequence_number}, "
            f"commit_count={len(senders)}, "
            f"threshold={self.commit_threshold}"
        )

        self._evaluate_committed(key)

    def _evaluate_committed(self, key: MessageKey) -> None:
        """Execute the request after PREPARED and COMMIT quorum."""
        if key in self.committed_instances:
            return

        # A COMMIT quorum alone is not sufficient. The local replica
        # must also have reached PREPARED.
        if key not in self.prepared_instances:
            return

        commit_count = len(self.commit_senders[key])

        if commit_count < self.commit_threshold:
            return

        self.committed_instances.add(key)

        instance = self.instances[key]
        view, sequence_number = key

        # Execute the replicated state transition.
        self.emergency_stop = instance.emergency_stop
        
        self.current_key = key
        self.phase = "COMMITTED"
        self._publish_status(
            "COMMIT quorum formed and emergency-stop state executed."
        )

        self.get_logger().info(
            "COMMITTED: "
            f"view={view}, "
            f"sequence={sequence_number}, "
            f"request_id={instance.request_id}, "
            f"commit_senders="
            f"{sorted(self.commit_senders[key])}"
        )

        self.get_logger().info(
            "STATE UPDATED: "
            f"emergency_stop={self.emergency_stop}"
        )


def main(args=None) -> None:
    """Run one PBFT replica."""
    rclpy.init(args=args)

    node = PBFTReplica()

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
