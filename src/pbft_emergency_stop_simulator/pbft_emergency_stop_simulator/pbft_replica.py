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
        self.declare_parameter("byzantine_behavior", "none")
        self.declare_parameter("duplicate_message_count", 3)
        self.declare_parameter("prepare_delay_sec", 4.0)
        self.declare_parameter("commit_delay_sec", 4.0)

        self.node_id = int(
            self.get_parameter("node_id").value
        )
        self.configured_primary_id = int(
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
        self.byzantine_behavior = str(
            self.get_parameter("byzantine_behavior").value
        ).strip().lower()

        self.duplicate_message_count = int(
            self.get_parameter("duplicate_message_count").value
        )
        self.prepare_delay_sec = float(
            self.get_parameter("prepare_delay_sec").value
        )
        self.commit_delay_sec = float(
            self.get_parameter("commit_delay_sec").value
        )

        if self.duplicate_message_count < 2:
            raise ValueError(
                "duplicate_message_count must be at least 2."
            )

        if self.prepare_delay_sec < 0.0:
            raise ValueError(
                "prepare_delay_sec must be non-negative."
            )

        if self.commit_delay_sec < 0.0:
            raise ValueError(
                "commit_delay_sec must be non-negative."
            )

        allowed_behaviors = {
            "none",
            "silent",
            "bad_digest",
            "duplicate",
            "equivocation",
            "skip_prepare",
            "skip_commit",
            "delayed_prepare",
            "delayed_commit",
            "early_commit",
            "wrong_sequence",
            "wrong_view",
            "wrong_value",
            "invalid_sender",
        }

        if self.byzantine_behavior not in allowed_behaviors:
            raise ValueError(
                "Unsupported byzantine_behavior="
                f"'{self.byzantine_behavior}'. "
                f"Allowed values: {sorted(allowed_behaviors)}"
            )

        if not self.is_byzantine:
            self.byzantine_behavior = "none"

        self._validate_configuration()

        # The active primary is derived from the current PBFT view.
        self.primary_id = self._primary_for_view(
            self.current_view
        )

        self.prepare_threshold = 2 * self.max_faulty
        self.commit_threshold = 2 * self.max_faulty + 1

        self.next_sequence_number = 1

        # Replicated application state.
        self.emergency_stop = False
        
        self.current_key: MessageKey | None = None

        if (
            self.is_byzantine
            and self.byzantine_behavior == "silent"
        ):
            self.phase = "SILENT"
            self.status_detail = (
                "Silent Byzantine mode enabled. "
                "Replica will not send PREPARE or COMMIT messages."
            )
        else:
            self.phase = "IDLE"
            self.status_detail = "Replica initialized."

        # REQUEST bookkeeping.

        # Every replica caches valid client requests so that a future
        # primary can continue them after a view change.
        self.cached_client_requests: dict[str, PBFTInstance,] = {}

        # Requests for which this replica has already started the
        # normal PBFT protocol while acting as primary.
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
        self.prepare_scheduled: set[MessageKey] = set()
        self.delayed_prepare_timers: dict[MessageKey, object] = {}
        self.prepared_instances: set[MessageKey] = set()

        # COMMIT state.
        self.commit_senders: dict[
            MessageKey, set[int]
        ] = defaultdict(set)

        self.pending_commits: dict[
            MessageKey, dict[int, PBFTMessage]
        ] = defaultdict(dict)

        self.commit_sent: set[MessageKey] = set()
        self.commit_scheduled: set[MessageKey] = set()
        self.delayed_commit_timers: dict[MessageKey, object] = {}
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
            f"emergency_stop={self.emergency_stop}, "
            f"is_byzantine={self.is_byzantine}, "
            f"byzantine_behavior={self.byzantine_behavior}, "
            f"prepare_delay_sec={self.prepare_delay_sec}, "
            f"commit_delay_sec={self.commit_delay_sec}"
        )
        
        self._publish_status(self.status_detail)
        
    
    
    
    def _is_silent_byzantine(self) -> bool:
        """Return whether this replica simulates a silent fault."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "silent"
        )


    def _is_skip_prepare_byzantine(self) -> bool:
        """Return whether this replica intentionally skips PREPARE."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "skip_prepare"
        )

    def _is_skip_commit_byzantine(self) -> bool:
        """Return whether this replica intentionally skips COMMIT."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "skip_commit"
        )

    def _is_delayed_prepare_byzantine(self) -> bool:
        """Return whether this replica delays its PREPARE."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "delayed_prepare"
        )

    def _is_delayed_commit_byzantine(self) -> bool:
        """Return whether this replica delays its COMMIT."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "delayed_commit"
        )

    def _is_early_commit_byzantine(self) -> bool:
        """Return whether this replica sends COMMIT before PREPARED."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "early_commit"
        )

    def _is_wrong_sequence_byzantine(self) -> bool:
        """Return whether this replica sends sequence_number=0."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "wrong_sequence"
        )

    def _is_wrong_view_byzantine(self) -> bool:
        """Return whether this replica sends a future view number."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "wrong_view"
        )

    def _is_wrong_value_byzantine(self) -> bool:
        """Return whether this replica sends emergency_stop=false."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "wrong_value"
        )

    def _is_invalid_sender_byzantine(self) -> bool:
        """Return whether this replica uses an out-of-range sender_id."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "invalid_sender"
        )


    def _is_bad_digest_byzantine(self) -> bool:
        """Return whether this replica corrupts outgoing digests."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "bad_digest"
        )
    
    def _is_equivocation_byzantine(self) -> bool:
        """Return whether this replica sends conflicting messages."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "equivocation"
        )


    def _publish_equivocating_message(
        self,
        message_type: int,
        key: MessageKey,
    ) -> None:
        """Send conflicting messages to different replicas."""
        instance = self.instances[key]
        view, sequence_number = key

        if message_type == PBFTMessage.PREPARE:
            publisher = self.prepare_publisher
            phase_name = "PREPARE"
        elif message_type == PBFTMessage.COMMIT:
            publisher = self.commit_publisher
            phase_name = "COMMIT"
        else:
            raise ValueError(
                "Equivocation is supported only for "
                "PREPARE and COMMIT messages."
            )

        # A conflicting request which is internally valid,
        # but does not match the accepted PRE-PREPARE.
        conflicting_request_id = (
            f"{instance.request_id}-conflict"
        )

        conflicting_digest = compute_request_digest(
            conflicting_request_id,
            instance.emergency_stop,
        )

        recipients = [
            replica_id
            for replica_id in range(self.replica_count)
            if replica_id != self.node_id
        ]

        # In the current n=4 scenario, node 1 receives
        # the conflicting value.
        conflict_recipient = (
            1 if 1 in recipients else recipients[0]
        )

        for recipient_id in recipients:
            is_conflicting = (
                recipient_id == conflict_recipient
            )

            message = PBFTMessage()

            message.stamp = self.get_clock().now().to_msg()
            message.message_type = message_type
            message.sender_id = self.node_id
            message.recipient_id = recipient_id
            message.view = view
            message.sequence_number = sequence_number
            message.emergency_stop = (
                instance.emergency_stop
            )

            if is_conflicting:
                message.request_id = (
                    conflicting_request_id
                )
                message.request_digest = (
                    conflicting_digest
                )
                variant = "CONFLICTING"
            else:
                message.request_id = instance.request_id
                message.request_digest = (
                    instance.request_digest
                )
                variant = "ORIGINAL"

            publisher.publish(message)

            self.get_logger().warning(
                "EQUIVOCATION BYZANTINE BEHAVIOR: "
                f"published {phase_name}, "
                f"recipient={recipient_id}, "
                f"variant={variant}, "
                f"view={view}, "
                f"sequence={sequence_number}, "
                f"request_id={message.request_id}, "
                f"digest={message.request_digest[:12]}..."
            )


    def _is_duplicate_byzantine(self) -> bool:
        """Return whether this replica duplicates outgoing messages."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "duplicate"
        )

    def _outgoing_sender_id(self, correct_sender_id: int) -> int:
        """Return the sender ID placed in an outgoing message."""
        if self._is_invalid_sender_byzantine():
            # For n=4, sender_id=4 is outside the valid range 0..3.
            return self.replica_count

        return correct_sender_id

    def _outgoing_view(self, correct_view: int) -> int:
        """Return the view placed in an outgoing protocol message."""
        if self._is_wrong_view_byzantine():
            return correct_view + 1

        return correct_view

    def _outgoing_sequence_number(
        self,
        correct_sequence_number: int,
    ) -> int:
        """Return the sequence number placed in an outgoing message."""
        if self._is_wrong_sequence_byzantine():
            # Zero is reserved for the client REQUEST before the
            # primary assigns a real PBFT sequence number.
            return 0

        return correct_sequence_number

    def _outgoing_emergency_stop(
        self,
        correct_value: bool,
    ) -> bool:
        """Return the emergency-stop value placed in a message."""
        if self._is_wrong_value_byzantine():
            return not correct_value

        return correct_value

    def _outgoing_digest(
        self,
        request_id: str,
        correct_digest: str,
        outgoing_emergency_stop: bool,
    ) -> str:
        """Return a digest consistent with the selected fault mode."""
        if self._is_bad_digest_byzantine():
            corrupted_digest = "0" * 64

            if corrupted_digest == correct_digest:
                corrupted_digest = "f" * 64

            return corrupted_digest

        if self._is_wrong_value_byzantine():
            # Keep the digest internally valid for emergency_stop=false
            # so receivers reject the message because of the value,
            # not because of an unrelated digest error.
            return compute_request_digest(
                request_id,
                outgoing_emergency_stop,
            )

        return correct_digest

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

    
    
    def _primary_for_view(self, view: int) -> int:
        """Return the primary replica assigned to the given view."""
        if view < 0:
            raise ValueError(
                "PBFT view must be non-negative."
            )

        return view % self.replica_count
    
    
    
    def _validate_configuration(self) -> None:
        """Validate replica identity and the supported PBFT configuration."""
        if self.max_faulty < 0:
            raise ValueError(
                "max_faulty must be non-negative."
            )

        expected_replica_count = 3 * self.max_faulty + 1

        if self.replica_count != expected_replica_count:
            raise ValueError(
                "Invalid PBFT configuration: "
                f"n={self.replica_count}, "
                f"f={self.max_faulty}. "
                "This simulator currently requires "
                f"n = 3f + 1 = {expected_replica_count}."
            )

        if not 0 <= self.node_id < self.replica_count:
            raise ValueError(
                f"node_id={self.node_id} is outside the valid range "
                f"0..{self.replica_count - 1}."
            )

        if self.current_view < 0:
            raise ValueError(
                "current_view must be non-negative."
            )

        if not (
            0
            <= self.configured_primary_id
            < self.replica_count
        ):
            raise ValueError(
                "Configured primary_id="
                f"{self.configured_primary_id} is outside the valid "
                f"range 0..{self.replica_count - 1}."
            )

        expected_primary_id = self._primary_for_view(
            self.current_view
        )

        if self.configured_primary_id != expected_primary_id:
            raise ValueError(
                "Invalid initial primary configuration: "
                f"current_view={self.current_view}, "
                f"configured_primary_id="
                f"{self.configured_primary_id}, "
                f"expected_primary_id="
                f"{expected_primary_id}. "
                "The primary must satisfy "
                "primary_id = current_view % replica_count."
            )




    def _process_cached_request_as_primary(
        self,
        request_id: str,
    ) -> None:
        """Assign a sequence number and start normal PBFT processing."""
        instance = self.cached_client_requests.get(request_id)

        if instance is None:
            self.get_logger().error(
                "Primary cannot process an uncached REQUEST: "
                f"request_id={request_id}"
            )
            return

        sequence_number = self.next_sequence_number
        self.next_sequence_number += 1

        key = (
            self.current_view,
            sequence_number,
        )

        self.instances[key] = instance
        self.processed_request_ids.add(request_id)

        self.current_key = key
        self.phase = "PRE_PREPARED"

        self._publish_status(
            "Primary accepted cached REQUEST and assigned "
            "a sequence number."
        )

        self.get_logger().info(
            "Accepted valid REQUEST as primary: "
            f"request_id={instance.request_id}, "
            f"assigned_sequence={sequence_number}, "
            f"digest={instance.request_digest[:12]}..."
        )

        pre_prepare = PBFTMessage()

        pre_prepare.stamp = self.get_clock().now().to_msg()
        pre_prepare.message_type = PBFTMessage.PRE_PREPARE
        pre_prepare.sender_id = self.node_id
        pre_prepare.recipient_id = -1
        pre_prepare.view = self.current_view
        pre_prepare.sequence_number = sequence_number
        pre_prepare.request_id = instance.request_id
        pre_prepare.request_digest = instance.request_digest
        pre_prepare.emergency_stop = instance.emergency_stop

        self.pre_prepare_publisher.publish(pre_prepare)

        self.get_logger().info(
            "Published PRE-PREPARE: "
            f"view={pre_prepare.view}, "
            f"sequence={pre_prepare.sequence_number}, "
            f"request_id={pre_prepare.request_id}, "
            f"digest={pre_prepare.request_digest[:12]}..."
        )



    def _validate_and_cache_client_request(
        self,
        message: PBFTMessage,
    ) -> PBFTInstance | None:
        """Validate a client request and cache it on this replica."""
        if message.message_type != PBFTMessage.REQUEST:
            self.get_logger().warning(
                "Rejected message on /pbft/request: "
                f"message_type={message.message_type}"
            )
            return None

        if message.sender_id != -1:
            self.get_logger().warning(
                "Rejected REQUEST with an invalid client sender_id: "
                f"{message.sender_id}"
            )
            return None

        # The request is logically addressed to the current primary,
        # but every replica receives the ROS 2 topic and caches it.
        if message.recipient_id not in (-1, self.primary_id):
            self.get_logger().warning(
                "Rejected REQUEST intended for an unexpected primary: "
                f"recipient_id={message.recipient_id}, "
                f"expected_primary_id={self.primary_id}"
            )
            return None

        if message.view != self.current_view:
            self.get_logger().warning(
                "Rejected REQUEST with an invalid view: "
                f"received={message.view}, "
                f"expected={self.current_view}"
            )
            return None

        if message.sequence_number != 0:
            self.get_logger().warning(
                "Rejected REQUEST with a non-zero sequence number: "
                f"sequence_number={message.sequence_number}"
            )
            return None

        if not message.request_id:
            self.get_logger().warning(
                "Rejected REQUEST with an empty request_id."
            )
            return None

        expected_digest = compute_request_digest(
            message.request_id,
            message.emergency_stop,
        )

        if message.request_digest != expected_digest:
            self.get_logger().warning(
                "Rejected REQUEST because its digest is invalid: "
                f"request_id={message.request_id}"
            )
            return None

        if not message.emergency_stop:
            self.get_logger().warning(
                "Rejected REQUEST because this simulator currently "
                "supports only emergency_stop=true."
            )
            return None

        instance = PBFTInstance(
            request_id=message.request_id,
            request_digest=message.request_digest,
            emergency_stop=message.emergency_stop,
        )

        existing_instance = self.cached_client_requests.get(
            message.request_id
        )

        if existing_instance is not None:
            if existing_instance != instance:
                self.get_logger().error(
                    "Rejected conflicting client REQUEST reuse: "
                    f"request_id={message.request_id}"
                )
                return None

            # The same valid request is already cached. Return it so that
            # the primary can independently detect protocol-level replay.
            return existing_instance

        self.cached_client_requests[
            message.request_id
        ] = instance

        self.get_logger().info(
            "Cached valid client REQUEST: "
            f"request_id={message.request_id}, "
            f"view={message.view}, "
            f"intended_primary={message.recipient_id}, "
            f"digest={message.request_digest[:12]}..."
        )

        return instance



    def request_callback(self, message: PBFTMessage) -> None:
        """Validate and cache a client REQUEST on every replica."""
        instance = self._validate_and_cache_client_request(
            message
        )

        if instance is None:
            return

        # Every correct replica stores the request, but only the current
        # primary may assign a sequence number and start PRE-PREPARE.
        if self.node_id != self.primary_id:
            return

        if message.request_id in self.processed_request_ids:
            self.get_logger().warning(
                "Duplicate REQUEST ignored by the primary: "
                f"request_id={message.request_id}"
            )
            return

        self._process_cached_request_as_primary(
            message.request_id
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

        if self._is_silent_byzantine():
            self.phase = "SILENT"

            self._publish_status(
                "Valid PRE-PREPARE received, but silent Byzantine "
                "replica intentionally sends no PREPARE."
            )

            self.get_logger().warning(
                "SILENT BYZANTINE BEHAVIOR: "
                f"received PRE-PREPARE for view={message.view}, "
                f"sequence={message.sequence_number}, "
                "but no PREPARE will be published."
            )
            return

        if key not in self.prepared_instances:
            self.phase = "PRE_PREPARED"

        self._publish_status(
            "Valid PRE-PREPARE accepted."
        )

        # Process PREPARE messages which may have arrived before
        # PRE-PREPARE. COMMIT messages remain buffered until this
        # replica locally reaches PREPARED.
        self._process_pending_prepares(key)

        # A Byzantine replica may deliberately violate the protocol
        # by sending COMMIT before it has locally reached PREPARED.
        if self._is_early_commit_byzantine():
            self._send_early_commit(key)

        # The primary proposes the request but does not send PREPARE
        # in this simplified PBFT model.
        if self.node_id != self.primary_id:
            self._send_prepare(key)

    def _send_early_commit(self, key: MessageKey) -> None:
        """Publish COMMIT before PREPARED to test receiver safety."""
        if key in self.commit_sent:
            return

        instance = self.instances[key]
        view, sequence_number = key

        commit = PBFTMessage()

        commit.stamp = self.get_clock().now().to_msg()
        commit.message_type = PBFTMessage.COMMIT
        outgoing_emergency_stop = self._outgoing_emergency_stop(
            instance.emergency_stop
        )

        commit.sender_id = self._outgoing_sender_id(
            self.node_id
        )
        commit.recipient_id = -1
        commit.view = self._outgoing_view(view)
        commit.sequence_number = self._outgoing_sequence_number(
            sequence_number
        )
        commit.request_id = instance.request_id
        commit.request_digest = self._outgoing_digest(
            instance.request_id,
            instance.request_digest,
            outgoing_emergency_stop,
        )
        commit.emergency_stop = outgoing_emergency_stop

        # This is the replica's only COMMIT for this PBFT instance.
        # When it later becomes PREPARED, _send_commit() will not
        # publish the same sender vote again.
        self.commit_sent.add(key)

        # Store the local copy in the pending buffer. It must not
        # increase commit_count before this replica becomes PREPARED.
        self._buffer_early_commit(
            key,
            commit,
            reason="local replica has not reached PREPARED",
        )
        self.commit_publisher.publish(commit)

        self.get_logger().warning(
            "EARLY-COMMIT BYZANTINE BEHAVIOR: "
            f"replica={self.node_id}, "
            f"view={view}, "
            f"sequence={sequence_number}. "
            "Published COMMIT before the local PREPARED condition."
        )

    def _schedule_delayed_prepare(
        self,
        key: MessageKey,
    ) -> None:
        """Schedule one delayed PREPARE without blocking the executor."""
        if key in self.prepare_sent or key in self.prepare_scheduled:
            return

        self.prepare_scheduled.add(key)

        timer = self.create_timer(
            self.prepare_delay_sec,
            lambda scheduled_key=key: self._publish_delayed_prepare(
                scheduled_key
            ),
        )
        self.delayed_prepare_timers[key] = timer

        view, sequence_number = key

        self._publish_status(
            "PREPARE publication scheduled after a controlled delay."
        )

        self.get_logger().warning(
            "DELAYED-PREPARE BYZANTINE BEHAVIOR: "
            f"scheduled PREPARE after delay_sec="
            f"{self.prepare_delay_sec:.3f}, "
            f"view={view}, sequence={sequence_number}."
        )

    def _publish_delayed_prepare(
        self,
        key: MessageKey,
    ) -> None:
        """Publish a previously scheduled PREPARE exactly once."""
        timer = self.delayed_prepare_timers.pop(key, None)

        if timer is not None:
            timer.cancel()

        self.prepare_scheduled.discard(key)

        if key not in self.instances:
            self.get_logger().error(
                "Delayed PREPARE could not be published because "
                f"the instance no longer exists: key={key}."
            )
            return

        view, sequence_number = key

        self._send_prepare(
            key,
            bypass_delay=True,
        )

        self.get_logger().warning(
            "DELAYED-PREPARE BYZANTINE BEHAVIOR: "
            f"published PREPARE after delay_sec="
            f"{self.prepare_delay_sec:.3f}, "
            f"view={view}, sequence={sequence_number}."
        )

    def _send_prepare(
        self,
        key: MessageKey,
        bypass_delay: bool = False,
    ) -> None:
        """Publish one PREPARE for an accepted PRE-PREPARE."""
        if self._is_silent_byzantine():
            return
        
        if key in self.prepare_sent:
            return

        if (
            self._is_delayed_prepare_byzantine()
            and not bypass_delay
        ):
            self._schedule_delayed_prepare(key)
            return

        if self._is_skip_prepare_byzantine():
            view, sequence_number = key
            self.prepare_sent.add(key)

            self._publish_status(
                "Valid PRE-PREPARE accepted, but this Byzantine "
                "replica intentionally skipped its PREPARE message."
            )

            self.get_logger().warning(
                "SKIP-PREPARE BYZANTINE BEHAVIOR: "
                f"replica={self.node_id}, "
                f"view={view}, "
                f"sequence={sequence_number}. "
                "No PREPARE message was published."
            )
            return

        if self._is_equivocation_byzantine():
            self.prepare_sent.add(key)

            self._publish_equivocating_message(
                PBFTMessage.PREPARE,
                key,
            )
            return

        instance = self.instances[key]
        view, sequence_number = key

        prepare = PBFTMessage()

        prepare.stamp = self.get_clock().now().to_msg()
        prepare.message_type = PBFTMessage.PREPARE
        outgoing_emergency_stop = self._outgoing_emergency_stop(
            instance.emergency_stop
        )

        prepare.sender_id = self._outgoing_sender_id(
            self.node_id
        )
        prepare.recipient_id = -1
        prepare.view = self._outgoing_view(view)
        prepare.sequence_number = self._outgoing_sequence_number(
            sequence_number
        )
        prepare.request_id = instance.request_id
        prepare.request_digest = self._outgoing_digest(
            instance.request_id,
            instance.request_digest,
            outgoing_emergency_stop,
        )
        prepare.emergency_stop = outgoing_emergency_stop

        self.prepare_sent.add(key)

        # Do not bypass sender validation for an intentionally
        # invalid sender ID.
        if not self._is_invalid_sender_byzantine():
            self._accept_prepare(prepare)

        self.prepare_publisher.publish(prepare)


        if self._is_duplicate_byzantine():
            for _ in range(self.duplicate_message_count - 1):
                self.prepare_publisher.publish(prepare)

            self.get_logger().warning(
                "DUPLICATE BYZANTINE BEHAVIOR: "
                f"published the same PREPARE "
                f"{self.duplicate_message_count} times for "
                f"view={view}, sequence={sequence_number}."
            )

        if self._is_bad_digest_byzantine():
            self.get_logger().warning(
                "BAD-DIGEST BYZANTINE BEHAVIOR: "
                f"published PREPARE with corrupted digest for "
                f"view={view}, sequence={sequence_number}. "
                f"correct={instance.request_digest[:12]}..., "
                f"sent={prepare.request_digest[:12]}..."
            )

        if self._is_wrong_sequence_byzantine():
            self.get_logger().warning(
                "WRONG-SEQUENCE BYZANTINE BEHAVIOR: "
                f"published PREPARE with sequence=0, "
                f"expected_sequence={sequence_number}, "
                f"view={view}."
            )

        if self._is_wrong_view_byzantine():
            self.get_logger().warning(
                "WRONG-VIEW BYZANTINE BEHAVIOR: "
                f"published PREPARE with view={prepare.view}, "
                f"expected_view={view}, "
                f"sequence={sequence_number}."
            )

        if self._is_wrong_value_byzantine():
            self.get_logger().warning(
                "WRONG-VALUE BYZANTINE BEHAVIOR: "
                "published PREPARE with emergency_stop=false "
                f"for view={view}, sequence={sequence_number}."
            )

        if self._is_invalid_sender_byzantine():
            self.get_logger().warning(
                "INVALID-SENDER BYZANTINE BEHAVIOR: "
                f"published PREPARE with sender_id={prepare.sender_id}, "
                f"valid_range=0..{self.replica_count - 1}, "
                f"view={view}, sequence={sequence_number}."
            )

        self.get_logger().info(
            "Published PREPARE: "
            f"sender={prepare.sender_id}, "
            f"view={prepare.view}, "
            f"sequence={prepare.sequence_number}, "
            f"emergency_stop={prepare.emergency_stop}, "
            f"digest={prepare.request_digest[:12]}..."
        )

    def prepare_callback(self, message: PBFTMessage) -> None:
        """Validate and record an incoming PREPARE."""
        
        if self._is_silent_byzantine():
            return
        
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
            if message.sender_id != self.node_id:
                self.get_logger().warning(
                    "Duplicate PREPARE ignored: "
                    f"sender={message.sender_id}, "
                    f"view={message.view}, "
                    f"sequence={message.sequence_number}. "
                    "The sender is already counted."
                )
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

        # COMMIT messages received before PREPARED are validated
        # again and counted only now.
        self._process_pending_commits(key)

        # Every prepared replica broadcasts COMMIT.
        self._send_commit(key)

    def _schedule_delayed_commit(
        self,
        key: MessageKey,
    ) -> None:
        """Schedule one delayed COMMIT without blocking the executor."""
        if key in self.commit_sent or key in self.commit_scheduled:
            return

        self.commit_scheduled.add(key)

        timer = self.create_timer(
            self.commit_delay_sec,
            lambda scheduled_key=key: self._publish_delayed_commit(
                scheduled_key
            ),
        )
        self.delayed_commit_timers[key] = timer

        view, sequence_number = key

        self._publish_status(
            "COMMIT publication scheduled after a controlled delay."
        )

        self.get_logger().warning(
            "DELAYED-COMMIT BYZANTINE BEHAVIOR: "
            f"scheduled COMMIT after delay_sec="
            f"{self.commit_delay_sec:.3f}, "
            f"view={view}, sequence={sequence_number}."
        )

    def _publish_delayed_commit(
        self,
        key: MessageKey,
    ) -> None:
        """Publish a previously scheduled COMMIT exactly once."""
        timer = self.delayed_commit_timers.pop(key, None)

        if timer is not None:
            timer.cancel()

        self.commit_scheduled.discard(key)

        if key not in self.instances:
            self.get_logger().error(
                "Delayed COMMIT could not be published because "
                f"the instance no longer exists: key={key}."
            )
            return

        view, sequence_number = key

        self._send_commit(
            key,
            bypass_delay=True,
        )

        self.get_logger().warning(
            "DELAYED-COMMIT BYZANTINE BEHAVIOR: "
            f"published COMMIT after delay_sec="
            f"{self.commit_delay_sec:.3f}, "
            f"view={view}, sequence={sequence_number}."
        )

    def _send_commit(
        self,
        key: MessageKey,
        bypass_delay: bool = False,
    ) -> None:
        """Publish one COMMIT after entering PREPARED state."""
        if self._is_silent_byzantine():
            return

        if key in self.commit_sent:
            return

        if key not in self.prepared_instances:
            return

        if (
            self._is_delayed_commit_byzantine()
            and not bypass_delay
        ):
            self._schedule_delayed_commit(key)
            return

        if self._is_skip_commit_byzantine():
            view, sequence_number = key

            self._publish_status(
                "PREPARE quorum formed, but this Byzantine replica "
                "intentionally skipped its COMMIT message."
            )

            self.get_logger().warning(
                "SKIP-COMMIT BYZANTINE BEHAVIOR: "
                f"replica={self.node_id}, "
                f"view={view}, "
                f"sequence={sequence_number}. "
                "The replica remains PREPARED and sends no COMMIT."
            )
            return

        if self._is_equivocation_byzantine():
            self.commit_sent.add(key)

            self._publish_equivocating_message(
                PBFTMessage.COMMIT,
                key,
            )
            return

        instance = self.instances[key]
        view, sequence_number = key

        commit = PBFTMessage()

        commit.stamp = self.get_clock().now().to_msg()
        commit.message_type = PBFTMessage.COMMIT
        outgoing_emergency_stop = self._outgoing_emergency_stop(
            instance.emergency_stop
        )

        commit.sender_id = self._outgoing_sender_id(
            self.node_id
        )
        commit.recipient_id = -1
        commit.view = self._outgoing_view(view)
        commit.sequence_number = self._outgoing_sequence_number(
            sequence_number
        )
        commit.request_id = instance.request_id
        commit.request_digest = self._outgoing_digest(
            instance.request_id,
            instance.request_digest,
            outgoing_emergency_stop,
        )
        commit.emergency_stop = outgoing_emergency_stop

        self.commit_sent.add(key)

        # Do not bypass sender validation for an intentionally
        # invalid sender ID.
        if not self._is_invalid_sender_byzantine():
            self._accept_commit(commit)

        self.commit_publisher.publish(commit)


        if self._is_duplicate_byzantine():
            for _ in range(self.duplicate_message_count - 1):
                self.commit_publisher.publish(commit)

            self.get_logger().warning(
                "DUPLICATE BYZANTINE BEHAVIOR: "
                f"published the same COMMIT "
                f"{self.duplicate_message_count} times for "
                f"view={view}, sequence={sequence_number}."
            )

        if self._is_bad_digest_byzantine():
            self.get_logger().warning(
                "BAD-DIGEST BYZANTINE BEHAVIOR: "
                f"published COMMIT with corrupted digest for "
                f"view={view}, sequence={sequence_number}. "
                f"correct={instance.request_digest[:12]}..., "
                f"sent={commit.request_digest[:12]}..."
            )

        if self._is_wrong_sequence_byzantine():
            self.get_logger().warning(
                "WRONG-SEQUENCE BYZANTINE BEHAVIOR: "
                f"published COMMIT with sequence=0, "
                f"expected_sequence={sequence_number}, "
                f"view={view}."
            )

        if self._is_wrong_view_byzantine():
            self.get_logger().warning(
                "WRONG-VIEW BYZANTINE BEHAVIOR: "
                f"published COMMIT with view={commit.view}, "
                f"expected_view={view}, "
                f"sequence={sequence_number}."
            )

        if self._is_wrong_value_byzantine():
            self.get_logger().warning(
                "WRONG-VALUE BYZANTINE BEHAVIOR: "
                "published COMMIT with emergency_stop=false "
                f"for view={view}, sequence={sequence_number}."
            )

        if self._is_invalid_sender_byzantine():
            self.get_logger().warning(
                "INVALID-SENDER BYZANTINE BEHAVIOR: "
                f"published COMMIT with sender_id={commit.sender_id}, "
                f"valid_range=0..{self.replica_count - 1}, "
                f"view={view}, sequence={sequence_number}."
            )

        self.get_logger().info(
            "Published COMMIT: "
            f"sender={commit.sender_id}, "
            f"view={commit.view}, "
            f"sequence={commit.sequence_number}, "
            f"emergency_stop={commit.emergency_stop}, "
            f"digest={commit.request_digest[:12]}..."
        )

    def commit_callback(self, message: PBFTMessage) -> None:
        """Validate and record an incoming COMMIT."""
        if self._is_silent_byzantine():
            return
        

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
            self._buffer_early_commit(
                key,
                message,
                reason="PRE-PREPARE has not been accepted",
            )
            return

        if key not in self.prepared_instances:
            self._buffer_early_commit(
                key,
                message,
                reason="local replica has not reached PREPARED",
            )
            return

        self._accept_commit(message)

    def _buffer_early_commit(
        self,
        key: MessageKey,
        message: PBFTMessage,
        reason: str,
    ) -> None:
        """Store a valid COMMIT without counting it in the quorum."""
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

        pending_count = len(self.pending_commits[key])

        if key in self.instances:
            self.current_key = key
            self._publish_status(
                "Buffered COMMIT from replica "
                f"{message.sender_id} without counting it because "
                f"{reason}. Pending COMMIT count: {pending_count}."
            )

        self.get_logger().info(
            "BUFFERED COMMIT WITHOUT QUORUM COUNT: "
            f"sender={message.sender_id}, "
            f"view={message.view}, "
            f"sequence={message.sequence_number}, "
            f"reason={reason}, "
            f"pending_count={pending_count}, "
            f"commit_count={len(self.commit_senders.get(key, set()))}"
        )

    def _process_pending_commits(
        self,
        key: MessageKey,
    ) -> None:
        """Count buffered COMMIT messages after reaching PREPARED."""
        if key not in self.prepared_instances:
            return

        pending = self.pending_commits.pop(key, {})

        if pending:
            self.get_logger().info(
                "PROCESSING BUFFERED COMMITS: "
                f"view={key[0]}, "
                f"sequence={key[1]}, "
                f"count={len(pending)}"
            )

        for message in pending.values():
            self._accept_commit(message)

    def _accept_commit(self, message: PBFTMessage) -> None:
        """Compare a COMMIT with local state and count its sender."""
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

        # Defense in depth: no call path may count a COMMIT before
        # this replica has locally reached PREPARED.
        if key not in self.prepared_instances:
            self._buffer_early_commit(
                key,
                message,
                reason="local replica has not reached PREPARED",
            )
            return

        senders = self.commit_senders[key]

        if message.sender_id in senders:
            if message.sender_id != self.node_id:
                self.get_logger().warning(
                    "Duplicate COMMIT ignored: "
                    f"sender={message.sender_id}, "
                    f"view={message.view}, "
                    f"sequence={message.sequence_number}. "
                    "The sender is already counted."
                )
            return

        senders.add(message.sender_id)

        self.current_key = key
        self._publish_status(
            f"Accepted COMMIT from replica {message.sender_id}."
        )

        self.get_logger().info(
            "Accepted COMMIT after PREPARED: "
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
