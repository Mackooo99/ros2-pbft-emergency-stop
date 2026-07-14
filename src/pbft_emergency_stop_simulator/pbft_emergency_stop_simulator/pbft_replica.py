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
    NewView,
    ViewChange,
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
        self.declare_parameter("manual_view_change_target", -1)
        self.declare_parameter("manual_view_change_delay_sec", 2.0)
        self.declare_parameter("enable_progress_timeout", False)
        self.declare_parameter("progress_timeout_sec", 3.0)




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
        self.enable_progress_timeout = bool(
            self.get_parameter("enable_progress_timeout").value
        )
        self.progress_timeout_sec = float(
            self.get_parameter("progress_timeout_sec").value
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
        self.manual_view_change_target = int(
            self.get_parameter("manual_view_change_target").value
        )
        self.manual_view_change_delay_sec = float(
            self.get_parameter("manual_view_change_delay_sec").value
        )

        if self.progress_timeout_sec <= 0.0:
            raise ValueError(
                "progress_timeout_sec must be positive."
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

        if self.manual_view_change_delay_sec <= 0.0:
            raise ValueError(
                "manual_view_change_delay_sec must be positive."
            )

        if (
            self.manual_view_change_target != -1
            and self.manual_view_change_target
            <= self.current_view
        ):
            raise ValueError(
                "manual_view_change_target must be -1 or greater "
                "than current_view."
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
            "skip_pre_prepare",
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
        self.view_change_threshold = 2 * self.max_faulty + 1


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


        # VIEW-CHANGE state indexed by the requested new view.
        # For every new view, at most one message from each sender is stored.
        self.view_change_messages: dict[
            int,
            dict[int, ViewChange],
        ] = defaultdict(dict)

        # Views for which this replica has already published its own
        # VIEW-CHANGE message. Publishing will be implemented later.
        self.view_change_sent: set[int] = set()

        # Views for which the new primary has already published NEW-VIEW.
        self.new_view_sent: set[int] = set()


        # Protocol-relevant payload of every accepted NEW-VIEW message.
        # It is used to ignore identical duplicates and detect conflicts.
        self.accepted_new_view_payloads: dict[int, tuple] = {}

        # Test-only timer used to invoke the same function that will
        # later be called by the real progress timeout.
        self.manual_view_change_timer = None


        # Progress timeout used to detect a stalled PBFT instance.
        self.progress_timeout_timer = None

        # Request and view currently protected by the timer.
        self.progress_timeout_request_id: str | None = None
        self.progress_timeout_view: int | None = None

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

        self.view_change_publisher = self.create_publisher(
            ViewChange,
            "/pbft/view_change",
            create_pbft_qos(),
        )

        self.new_view_publisher = self.create_publisher(
            NewView,
            "/pbft/new_view",
            create_pbft_qos(),
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

        self.view_change_subscription = self.create_subscription(
            ViewChange,
            "/pbft/view_change",
            self.view_change_callback,
            create_pbft_qos(),
        )


        self.new_view_subscription = self.create_subscription(
            NewView,
            "/pbft/new_view",
            self.new_view_callback,
            create_pbft_qos(),
        )

        
        self.status_timer = self.create_timer(
	    1.0,
	    self._publish_status,
	    )






        if self.manual_view_change_target > self.current_view:
            self.manual_view_change_timer = self.create_timer(
                self.manual_view_change_delay_sec,
                self._manual_view_change_timer_callback,
            )

            self.get_logger().info(
                "Manual VIEW-CHANGE trigger configured: "
                f"target_view={self.manual_view_change_target}, "
                f"delay_sec={self.manual_view_change_delay_sec:.3f}"
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
            f"commit_delay_sec={self.commit_delay_sec},"
            f", enable_progress_timeout="
            f"{self.enable_progress_timeout}, "
            f"progress_timeout_sec="
            f"{self.progress_timeout_sec}"
        )
        
        self._publish_status(self.status_detail)
        
    
    

    def _is_skip_pre_prepare_byzantine(
        self,
    ) -> bool:
        """Return whether a faulty primary skips PRE-PREPARE."""
        return (
            self.is_byzantine
            and self.byzantine_behavior == "skip_pre_prepare"
        )    

    
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

        self._arm_progress_timeout(
            instance.request_id,
            reason="primary published PRE-PREPARE",
        )

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


    def _select_prepared_instance_for_view_change(
        self,
        new_view: int,
    ) -> tuple[MessageKey, PBFTInstance] | None:
        """Select the newest unfinished locally PREPARED instance."""
        candidates = [
            key
            for key in self.prepared_instances
            if (
                key in self.instances
                and key not in self.committed_instances
                and key[0] < new_view
            )
        ]

        if not candidates:
            return None

        # The project currently processes one request at a time.
        # This selects the highest prepared view, then the highest
        # sequence number inside that view.
        selected_key = max(
            candidates,
            key=lambda key: (
                key[0],
                key[1],
            ),
        )

        return (
            selected_key,
            self.instances[selected_key],
        )



    def _validate_view_change_certificate(
    self,
    message: ViewChange,
) -> bool:
        """Validate the optional PREPARED certificate in VIEW-CHANGE."""
        if not message.has_prepared_certificate:
            certificate_is_empty = (
                message.prepared_sequence_number == 0
                and not message.request_id
                and not message.request_digest
                and not message.emergency_stop
                and len(message.prepare_senders) == 0
            )


            certificate_is_empty = (
                message.prepared_view == 0
                and message.prepared_sequence_number == 0
                and not message.request_id
                and not message.request_digest
                and not message.emergency_stop
                and len(message.prepare_senders) == 0
            )

            if not certificate_is_empty:
                self.get_logger().warning(
                    "Rejected VIEW-CHANGE with inconsistent empty "
                    "PREPARED certificate: "
                    f"sender={message.sender_id}, "
                    f"new_view={message.new_view}."
                )
                return False

            return True

        if message.prepared_view >= message.new_view:
            self.get_logger().warning(
                "Rejected VIEW-CHANGE because prepared_view must be "
                "older than new_view: "
                f"prepared_view={message.prepared_view}, "
                f"new_view={message.new_view}."
            )
            return False

        if message.prepared_sequence_number == 0:
            self.get_logger().warning(
                "Rejected VIEW-CHANGE with a PREPARED certificate "
                "and sequence_number=0."
            )
            return False

        if not message.request_id:
            self.get_logger().warning(
                "Rejected VIEW-CHANGE with an empty prepared request_id."
            )
            return False

        if not message.emergency_stop:
            self.get_logger().warning(
                "Rejected VIEW-CHANGE because the prepared request has "
                "emergency_stop=false."
            )
            return False

        expected_digest = compute_request_digest(
            message.request_id,
            message.emergency_stop,
        )

        if message.request_digest != expected_digest:
            self.get_logger().warning(
                "Rejected VIEW-CHANGE because the prepared request "
                "digest is invalid: "
                f"sender={message.sender_id}."
            )
            return False

        prepare_senders = list(message.prepare_senders)
        unique_prepare_senders = set(prepare_senders)

        if len(unique_prepare_senders) != len(prepare_senders):
            self.get_logger().warning(
                "Rejected VIEW-CHANGE because the PREPARED certificate "
                "contains duplicate PREPARE senders."
            )
            return False

        if len(unique_prepare_senders) < self.prepare_threshold:
            self.get_logger().warning(
                "Rejected VIEW-CHANGE because the PREPARED certificate "
                "does not contain enough PREPARE senders: "
                f"received={len(unique_prepare_senders)}, "
                f"required={self.prepare_threshold}."
            )
            return False

        prepared_primary = self._primary_for_view(
            message.prepared_view
        )

        for sender_id in unique_prepare_senders:
            if not 0 <= sender_id < self.replica_count:
                self.get_logger().warning(
                    "Rejected VIEW-CHANGE because the PREPARED "
                    "certificate contains an invalid sender_id: "
                    f"{sender_id}."
                )
                return False

            # In the current simplified normal-case implementation,
            # primary replicas do not send PREPARE.
            if sender_id == prepared_primary:
                self.get_logger().warning(
                    "Rejected VIEW-CHANGE because the PREPARED "
                    "certificate contains the primary as a PREPARE "
                    f"sender: sender_id={sender_id}, "
                    f"prepared_view={message.prepared_view}."
                )
                return False

        return True



    def _build_view_change_message(
        self,
        new_view: int,
    ) -> ViewChange:
        """Construct this replica's VIEW-CHANGE message."""
        if new_view <= self.current_view:
            raise ValueError(
                "A VIEW-CHANGE target must be greater than "
                "the current view."
            )

        message = ViewChange()

        message.stamp = self.get_clock().now().to_msg()
        message.sender_id = self.node_id
        message.new_view = new_view

        selected = (
            self._select_prepared_instance_for_view_change(
                new_view
            )
        )

        if selected is None:
            message.has_prepared_certificate = False
            message.prepared_view = 0
            message.prepared_sequence_number = 0
            message.request_id = ""
            message.request_digest = ""
            message.emergency_stop = False
            message.prepare_senders = []

            return message

        key, instance = selected
        prepared_view, sequence_number = key

        prepare_senders = sorted(
            self.prepare_senders.get(key, set())
        )

        if len(prepare_senders) < self.prepare_threshold:
            raise RuntimeError(
                "Local PREPARED invariant violated: "
                f"key={key}, "
                f"prepare_count={len(prepare_senders)}, "
                f"required={self.prepare_threshold}."
            )

        message.has_prepared_certificate = True
        message.prepared_view = prepared_view
        message.prepared_sequence_number = (
            sequence_number
        )
        message.request_id = instance.request_id
        message.request_digest = (
            instance.request_digest
        )
        message.emergency_stop = (
            instance.emergency_stop
        )
        message.prepare_senders = prepare_senders

        return message



    def _initiate_view_change(
        self,
        new_view: int,
        reason: str,
    ) -> None:
        """Publish this replica's VIEW-CHANGE exactly once."""
        if new_view <= self.current_view:
            self.get_logger().warning(
                "VIEW-CHANGE initiation ignored because the "
                "target view is not newer: "
                f"new_view={new_view}, "
                f"current_view={self.current_view}."
            )
            return

        if new_view in self.view_change_sent:
            self.get_logger().warning(
                "Local VIEW-CHANGE already sent: "
                f"sender={self.node_id}, "
                f"new_view={new_view}."
            )
            return

        if self._is_silent_byzantine():
            self.get_logger().warning(
                "Silent Byzantine replica intentionally skipped "
                "VIEW-CHANGE publication: "
                f"node_id={self.node_id}, "
                f"new_view={new_view}."
            )
            return

        message = self._build_view_change_message(
            new_view
        )


        if not self._validate_view_change_certificate(message):
            raise RuntimeError(
                "Refusing to publish a locally constructed invalid "
                f"VIEW-CHANGE message for new_view={new_view}."
            )

        self.view_change_sent.add(new_view)

        self.phase = "VIEW_CHANGE"
        self._publish_status(
            "Replica initiated VIEW-CHANGE: "
            f"target_view={new_view}, reason={reason}."
        )

        # Count the local message immediately instead of depending
        # on DDS loopback delivery.
        self.view_change_callback(message)

        self.view_change_publisher.publish(message)

        self.get_logger().warning(
            "Published VIEW-CHANGE: "
            f"sender={message.sender_id}, "
            f"new_view={message.new_view}, "
            f"reason={reason}, "
            f"has_prepared_certificate="
            f"{message.has_prepared_certificate}, "
            f"prepared_view={message.prepared_view}, "
            f"prepared_sequence="
            f"{message.prepared_sequence_number}, "
            f"prepare_senders="
            f"{list(message.prepare_senders)}"
        )

    def _view_change_payload(
        self,
        message: ViewChange,
    ) -> tuple:
        """Return the protocol-relevant VIEW-CHANGE payload."""
        return (
            message.new_view,
            message.has_prepared_certificate,
            message.prepared_view,
            message.prepared_sequence_number,
            message.request_id,
            message.request_digest,
            message.emergency_stop,
            tuple(message.prepare_senders),
        )    



    def _new_view_payload(
        self,
        message: NewView,
    ) -> tuple:
        """Return the protocol-relevant NEW-VIEW payload."""
        proof_payloads = tuple(
            sorted(
                (
                    view_change.sender_id,
                    self._view_change_payload(view_change),
                )
                for view_change
                in message.view_change_messages
            )
        )

        return (
            message.sender_id,
            message.new_view,
            proof_payloads,
            message.has_selected_request,
            message.selected_from_prepared_certificate,
            message.selected_prepared_view,
            message.selected_sequence_number,
            message.request_id,
            message.request_digest,
            message.emergency_stop,
        )



    def _select_request_for_new_view(
        self,
        new_view: int,
        view_change_messages: list[ViewChange],
    ) -> tuple[
        PBFTInstance,
        bool,
        int,
        int,
    ] | None:
        """
        Select the request which the new primary must preserve.

        Returns:
            (
                selected_instance,
                selected_from_prepared_certificate,
                selected_prepared_view,
                selected_sequence_number,
            )
        """
        prepared_messages = [
            message
            for message in view_change_messages
            if message.has_prepared_certificate
        ]

        # Safety rule: a value prepared in the highest prepared view
        # must be preserved by the new primary.
        if prepared_messages:
            highest_prepared_view = max(
                message.prepared_view
                for message in prepared_messages
            )

            highest_view_certificates = [
                message
                for message in prepared_messages
                if (
                    message.prepared_view
                    == highest_prepared_view
                )
            ]

            candidate_payloads = {
                (
                    message.prepared_sequence_number,
                    message.request_id,
                    message.request_digest,
                    message.emergency_stop,
                )
                for message in highest_view_certificates
            }

            if len(candidate_payloads) != 1:
                self.get_logger().error(
                    "Cannot construct NEW-VIEW because the highest "
                    "PREPARED certificates contain conflicting "
                    "requests: "
                    f"new_view={new_view}, "
                    f"highest_prepared_view="
                    f"{highest_prepared_view}, "
                    f"candidate_count="
                    f"{len(candidate_payloads)}."
                )
                return None

            (
                selected_sequence_number,
                request_id,
                request_digest,
                emergency_stop,
            ) = next(iter(candidate_payloads))

            instance = PBFTInstance(
                request_id=request_id,
                request_digest=request_digest,
                emergency_stop=emergency_stop,
            )

            return (
                instance,
                True,
                highest_prepared_view,
                selected_sequence_number,
            )

        # No request was PREPARED in an older view. The new primary
        # may start the one outstanding cached client request.
        committed_request_ids = {
            self.instances[key].request_id
            for key in self.committed_instances
            if key in self.instances
        }

        eligible_cached_requests = [
            instance
            for request_id, instance
            in self.cached_client_requests.items()
            if request_id not in committed_request_ids
        ]

        if len(eligible_cached_requests) != 1:
            self.get_logger().error(
                "Cannot construct NEW-VIEW without a PREPARED "
                "certificate because exactly one outstanding cached "
                "request is required: "
                f"new_view={new_view}, "
                f"eligible_request_count="
                f"{len(eligible_cached_requests)}."
            )
            return None

        selected_instance = eligible_cached_requests[0]

        return (
            selected_instance,
            False,
            0,
            self.next_sequence_number,
        )





    def _build_new_view_message(
        self,
        new_view: int,
        view_change_messages: list[ViewChange],
    ) -> NewView | None:
        """Build a NEW-VIEW message from a valid VIEW-CHANGE quorum."""
        expected_primary = self._primary_for_view(
            new_view
        )

        if self.node_id != expected_primary:
            self.get_logger().error(
                "A non-primary replica attempted to construct "
                "NEW-VIEW: "
                f"node_id={self.node_id}, "
                f"new_view={new_view}, "
                f"expected_primary={expected_primary}."
            )
            return None

        if new_view <= self.current_view:
            self.get_logger().warning(
                "Refusing to construct stale NEW-VIEW: "
                f"new_view={new_view}, "
                f"current_view={self.current_view}."
            )
            return None

        sender_ids = [
            message.sender_id
            for message in view_change_messages
        ]

        unique_sender_ids = set(sender_ids)

        if len(unique_sender_ids) != len(sender_ids):
            self.get_logger().error(
                "Cannot construct NEW-VIEW because the proof "
                "contains duplicate VIEW-CHANGE senders."
            )
            return None

        if (
            len(unique_sender_ids)
            < self.view_change_threshold
        ):
            self.get_logger().warning(
                "Cannot construct NEW-VIEW without a "
                "VIEW-CHANGE quorum: "
                f"new_view={new_view}, "
                f"received={len(unique_sender_ids)}, "
                f"required={self.view_change_threshold}."
            )
            return None

        for view_change in view_change_messages:
            if view_change.new_view != new_view:
                self.get_logger().error(
                    "Cannot construct NEW-VIEW because a proof "
                    "belongs to another target view: "
                    f"expected_new_view={new_view}, "
                    f"received_new_view="
                    f"{view_change.new_view}, "
                    f"sender={view_change.sender_id}."
                )
                return None

            if not (
                0
                <= view_change.sender_id
                < self.replica_count
            ):
                self.get_logger().error(
                    "Cannot construct NEW-VIEW because a proof "
                    "contains an invalid sender: "
                    f"sender={view_change.sender_id}."
                )
                return None

            if not self._validate_view_change_certificate(
                view_change
            ):
                self.get_logger().error(
                    "Cannot construct NEW-VIEW because a "
                    "VIEW-CHANGE certificate is invalid: "
                    f"sender={view_change.sender_id}, "
                    f"new_view={new_view}."
                )
                return None

        selection = self._select_request_for_new_view(
            new_view,
            view_change_messages,
        )

        if selection is None:
            return None

        (
            selected_instance,
            selected_from_prepared_certificate,
            selected_prepared_view,
            selected_sequence_number,
        ) = selection

        new_view_message = NewView()

        new_view_message.stamp = (
            self.get_clock().now().to_msg()
        )
        new_view_message.sender_id = self.node_id
        new_view_message.new_view = new_view
        new_view_message.view_change_messages = list(
            view_change_messages
        )

        new_view_message.has_selected_request = True
        new_view_message.selected_from_prepared_certificate = (
            selected_from_prepared_certificate
        )
        new_view_message.selected_prepared_view = (
            selected_prepared_view
        )
        new_view_message.selected_sequence_number = (
            selected_sequence_number
        )
        new_view_message.request_id = (
            selected_instance.request_id
        )
        new_view_message.request_digest = (
            selected_instance.request_digest
        )
        new_view_message.emergency_stop = (
            selected_instance.emergency_stop
        )

        return new_view_message



    def _maybe_publish_new_view(
        self,
        new_view: int,
    ) -> None:
        """Publish NEW-VIEW once the designated primary has a quorum."""
        if new_view <= self.current_view:
            return

        expected_primary = self._primary_for_view(
            new_view
        )

        # Only the primary assigned to the target view may publish
        # the corresponding NEW-VIEW message.
        if self.node_id != expected_primary:
            return

        if new_view in self.new_view_sent:
            return

        messages_for_view = self.view_change_messages.get(
            new_view,
            {},
        )

        if (
            len(messages_for_view)
            < self.view_change_threshold
        ):
            return

        proof_sender_ids = sorted(
            messages_for_view
        )

        proof_messages = [
            messages_for_view[sender_id]
            for sender_id in proof_sender_ids
        ]

        self.get_logger().info(
            "VIEW-CHANGE quorum reached by the new primary: "
            f"node_id={self.node_id}, "
            f"new_view={new_view}, "
            f"count={len(proof_sender_ids)}, "
            f"threshold={self.view_change_threshold}, "
            f"senders={proof_sender_ids}"
        )

        message = self._build_new_view_message(
            new_view,
            proof_messages,
        )

        if message is None:
            self.get_logger().error(
                "NEW-VIEW was not published because safe request "
                "selection or proof validation failed: "
                f"new_view={new_view}."
            )
            return

        # Mark the view before publication to prevent duplicate
        # NEW-VIEW messages if another VIEW-CHANGE arrives.
        self.new_view_sent.add(new_view)

        self.phase = "NEW_VIEW_PROPOSED"

        self._publish_status(
            "This replica formed a valid VIEW-CHANGE quorum and "
            f"published NEW-VIEW for view {new_view}."
        )

        self.new_view_publisher.publish(message)

        proof_senders = [
            view_change.sender_id
            for view_change in message.view_change_messages
        ]

        self.get_logger().warning(
            "Published NEW-VIEW: "
            f"sender={message.sender_id}, "
            f"new_view={message.new_view}, "
            f"proof_senders={sorted(proof_senders)}, "
            f"selected_from_prepared_certificate="
            f"{message.selected_from_prepared_certificate}, "
            f"selected_prepared_view="
            f"{message.selected_prepared_view}, "
            f"selected_sequence="
            f"{message.selected_sequence_number}, "
            f"request_id={message.request_id}, "
            f"digest={message.request_digest[:12]}..."
        )


    def _validate_new_view_message(
        self,
        message: NewView,
    ) -> tuple[PBFTInstance, int] | None:
        """
        Validate NEW-VIEW and return the selected instance and sequence.

        No local protocol state is changed by this function.
        """
        if message.new_view <= self.current_view:
            self.get_logger().warning(
                "Rejected stale NEW-VIEW: "
                f"sender={message.sender_id}, "
                f"new_view={message.new_view}, "
                f"current_view={self.current_view}."
            )
            return None

        expected_primary = self._primary_for_view(
            message.new_view
        )

        if message.sender_id != expected_primary:
            self.get_logger().warning(
                "Rejected NEW-VIEW because it was not sent by "
                "the primary assigned to the target view: "
                f"sender={message.sender_id}, "
                f"new_view={message.new_view}, "
                f"expected_primary={expected_primary}."
            )
            return None

        view_change_messages = list(
            message.view_change_messages
        )

        if (
            len(view_change_messages)
            < self.view_change_threshold
        ):
            self.get_logger().warning(
                "Rejected NEW-VIEW without enough VIEW-CHANGE "
                "proof messages: "
                f"new_view={message.new_view}, "
                f"received={len(view_change_messages)}, "
                f"required={self.view_change_threshold}."
            )
            return None

        proof_sender_ids = [
            view_change.sender_id
            for view_change in view_change_messages
        ]

        unique_proof_sender_ids = set(
            proof_sender_ids
        )

        if (
            len(unique_proof_sender_ids)
            != len(proof_sender_ids)
        ):
            self.get_logger().warning(
                "Rejected NEW-VIEW because its proof contains "
                "duplicate VIEW-CHANGE senders: "
                f"senders={proof_sender_ids}."
            )
            return None

        for view_change in view_change_messages:
            if (
                view_change.new_view
                != message.new_view
            ):
                self.get_logger().warning(
                    "Rejected NEW-VIEW because an embedded "
                    "VIEW-CHANGE belongs to another target view: "
                    f"new_view={message.new_view}, "
                    f"proof_new_view={view_change.new_view}, "
                    f"proof_sender={view_change.sender_id}."
                )
                return None

            if not (
                0
                <= view_change.sender_id
                < self.replica_count
            ):
                self.get_logger().warning(
                    "Rejected NEW-VIEW because an embedded "
                    "VIEW-CHANGE has an invalid sender_id: "
                    f"{view_change.sender_id}."
                )
                return None

            if not self._validate_view_change_certificate(
                view_change
            ):
                self.get_logger().warning(
                    "Rejected NEW-VIEW because an embedded "
                    "VIEW-CHANGE certificate is invalid: "
                    f"proof_sender={view_change.sender_id}, "
                    f"new_view={message.new_view}."
                )
                return None

        if not message.has_selected_request:
            self.get_logger().warning(
                "Rejected NEW-VIEW without a selected request."
            )
            return None

        if not message.request_id:
            self.get_logger().warning(
                "Rejected NEW-VIEW with an empty request_id."
            )
            return None

        if message.selected_sequence_number == 0:
            self.get_logger().warning(
                "Rejected NEW-VIEW with "
                "selected_sequence_number=0."
            )
            return None

        if not message.emergency_stop:
            self.get_logger().warning(
                "Rejected NEW-VIEW with emergency_stop=false."
            )
            return None

        expected_digest = compute_request_digest(
            message.request_id,
            message.emergency_stop,
        )

        if message.request_digest != expected_digest:
            self.get_logger().warning(
                "Rejected NEW-VIEW because the selected request "
                "digest is invalid: "
                f"request_id={message.request_id}."
            )
            return None

        expected_selection = (
            self._select_request_for_new_view(
                message.new_view,
                view_change_messages,
            )
        )

        if expected_selection is None:
            self.get_logger().warning(
                "Rejected NEW-VIEW because no safe request "
                "selection could be derived from its proof."
            )
            return None

        (
            expected_instance,
            expected_from_prepared,
            expected_prepared_view,
            expected_sequence_number,
        ) = expected_selection

        if (
            message.selected_from_prepared_certificate
            != expected_from_prepared
        ):
            self.get_logger().warning(
                "Rejected NEW-VIEW because "
                "selected_from_prepared_certificate is incorrect: "
                f"received="
                f"{message.selected_from_prepared_certificate}, "
                f"expected={expected_from_prepared}."
            )
            return None

        if (
            message.selected_prepared_view
            != expected_prepared_view
        ):
            self.get_logger().warning(
                "Rejected NEW-VIEW because selected_prepared_view "
                "does not match the safe selection: "
                f"received={message.selected_prepared_view}, "
                f"expected={expected_prepared_view}."
            )
            return None

        received_instance = PBFTInstance(
            request_id=message.request_id,
            request_digest=message.request_digest,
            emergency_stop=message.emergency_stop,
        )

        if received_instance != expected_instance:
            self.get_logger().warning(
                "Rejected NEW-VIEW because the selected request "
                "does not match the safe request derived from the "
                "VIEW-CHANGE proof: "
                f"received_request_id={message.request_id}, "
                f"expected_request_id="
                f"{expected_instance.request_id}."
            )
            return None

        if expected_from_prepared:
            if (
                message.selected_sequence_number
                != expected_sequence_number
            ):
                self.get_logger().warning(
                    "Rejected NEW-VIEW because the selected sequence "
                    "does not match the highest PREPARED certificate: "
                    f"received="
                    f"{message.selected_sequence_number}, "
                    f"expected={expected_sequence_number}."
                )
                return None
        else:
            # When no request was PREPARED, the new primary assigns
            # the positive sequence number carried by NEW-VIEW.
            if message.selected_prepared_view != 0:
                self.get_logger().warning(
                    "Rejected NEW-VIEW without a PREPARED "
                    "certificate but with non-zero "
                    "selected_prepared_view."
                )
                return None

        return (
            received_instance,
            message.selected_sequence_number,
        )



    def _cancel_obsolete_protocol_activity(
        self,
        new_view: int,
    ) -> None:
        """Cancel timers and buffers belonging to older PBFT views."""
        for key, timer in list(
            self.delayed_prepare_timers.items()
        ):
            if key[0] < new_view:
                timer.cancel()
                self.destroy_timer(timer)

                self.delayed_prepare_timers.pop(
                    key,
                    None,
                )
                self.prepare_scheduled.discard(key)

        for key, timer in list(
            self.delayed_commit_timers.items()
        ):
            if key[0] < new_view:
                timer.cancel()
                self.destroy_timer(timer)

                self.delayed_commit_timers.pop(
                    key,
                    None,
                )
                self.commit_scheduled.discard(key)

        for key in list(self.pending_prepares):
            if key[0] < new_view:
                self.pending_prepares.pop(
                    key,
                    None,
                )

        for key in list(self.pending_commits):
            if key[0] < new_view:
                self.pending_commits.pop(
                    key,
                    None,
                )

        if (
            self.manual_view_change_timer is not None
            and self.manual_view_change_target <= new_view
        ):
            self.manual_view_change_timer.cancel()
            self.destroy_timer(
                self.manual_view_change_timer
            )
            self.manual_view_change_timer = None




    def _activate_new_view(
        self,
        message: NewView,
        selected_instance: PBFTInstance,
        selected_sequence_number: int,
    ) -> None:
        """Install a previously validated NEW-VIEW locally."""
        old_view = self.current_view
        old_primary = self.primary_id

        new_view = message.new_view
        new_primary = self._primary_for_view(
            new_view
        )

        new_key = (
            new_view,
            selected_sequence_number,
        )

        existing_instance = self.instances.get(
            new_key
        )

        if (
            existing_instance is not None
            and existing_instance
            != selected_instance
        ):
            self.get_logger().error(
                "Refusing to activate NEW-VIEW because a "
                "conflicting local instance already exists: "
                f"key={new_key}, "
                f"existing_request_id="
                f"{existing_instance.request_id}, "
                f"selected_request_id="
                f"{selected_instance.request_id}."
            )
            return

        existing_cached_request = (
            self.cached_client_requests.get(
                selected_instance.request_id
            )
        )

        if (
            existing_cached_request is not None
            and existing_cached_request
            != selected_instance
        ):
            self.get_logger().error(
                "Refusing to activate NEW-VIEW because the "
                "selected request conflicts with the local cache: "
                f"request_id={selected_instance.request_id}."
            )
            return

        self._cancel_progress_timeout(
            reason="valid NEW-VIEW accepted",
        )

        self._cancel_obsolete_protocol_activity(
            new_view
        )

        self.cached_client_requests[
            selected_instance.request_id
        ] = selected_instance

        self.instances[new_key] = selected_instance

        # Prevent a repeated client REQUEST from starting another
        # sequence while this selected request is being recovered.
        self.processed_request_ids.add(
            selected_instance.request_id
        )

        self.current_view = new_view
        self.primary_id = new_primary
        self.current_key = new_key

        self.next_sequence_number = max(
            self.next_sequence_number,
            selected_sequence_number + 1,
        )

        self.phase = "NEW_VIEW_ACCEPTED"

        role = (
            "PRIMARY"
            if self.node_id == self.primary_id
            else "BACKUP"
        )

        self._publish_status(
            "Valid NEW-VIEW accepted and installed locally: "
            f"old_view={old_view}, "
            f"new_view={new_view}, "
            f"new_primary={new_primary}, "
            f"role={role}."
        )

        self.get_logger().warning(
            "Accepted and activated NEW-VIEW: "
            f"sender={message.sender_id}, "
            f"old_view={old_view}, "
            f"new_view={new_view}, "
            f"old_primary={old_primary}, "
            f"new_primary={new_primary}, "
            f"local_role={role}, "
            f"sequence={selected_sequence_number}, "
            f"request_id={selected_instance.request_id}, "
            f"selected_from_prepared_certificate="
            f"{message.selected_from_prepared_certificate}"
        )


    def new_view_callback(
        self,
        message: NewView,
    ) -> None:
        """Validate and install one incoming NEW-VIEW message."""
        received_payload = self._new_view_payload(
            message
        )

        existing_payload = (
            self.accepted_new_view_payloads.get(
                message.new_view
            )
        )

        if existing_payload is not None:
            if existing_payload == received_payload:
                if message.sender_id != self.node_id:
                    self.get_logger().warning(
                        "Duplicate NEW-VIEW ignored: "
                        f"sender={message.sender_id}, "
                        f"new_view={message.new_view}."
                    )
            else:
                self.get_logger().error(
                    "Conflicting NEW-VIEW messages detected for "
                    "the same target view: "
                    f"new_view={message.new_view}."
                )

            return

        validation_result = (
            self._validate_new_view_message(
                message
            )
        )

        if validation_result is None:
            return

        (
            selected_instance,
            selected_sequence_number,
        ) = validation_result

        # Store the payload before changing current_view, so an
        # identical DDS duplicate can be recognized afterwards.
        self.accepted_new_view_payloads[
            message.new_view
        ] = received_payload

        self._activate_new_view(
            message,
            selected_instance,
            selected_sequence_number,
        )





    def view_change_callback(
        self,
        message: ViewChange,
    ) -> None:
        """Validate and store one incoming VIEW-CHANGE message."""
        if not 0 <= message.sender_id < self.replica_count:
            self.get_logger().warning(
                "Rejected VIEW-CHANGE with an invalid sender_id: "
                f"{message.sender_id}."
            )
            return

        if message.new_view <= self.current_view:
            self.get_logger().warning(
                "Rejected stale VIEW-CHANGE: "
                f"sender={message.sender_id}, "
                f"new_view={message.new_view}, "
                f"current_view={self.current_view}."
            )
            return

        if not self._validate_view_change_certificate(message):
            return

        messages_for_view = self.view_change_messages[
            message.new_view
        ]

        existing_message = messages_for_view.get(
            message.sender_id
        )

        
        if existing_message is not None:
            existing_payload = self._view_change_payload(
                existing_message
            )
            received_payload = self._view_change_payload(
                message
            )

            if existing_payload == received_payload:
                # A locally stored message may later arrive again through
                # DDS loopback. That is expected and should not generate
                # a duplicate warning on the original sender.
                if message.sender_id != self.node_id:
                    self.get_logger().warning(
                        "Duplicate VIEW-CHANGE ignored: "
                        f"sender={message.sender_id}, "
                        f"new_view={message.new_view}."
                    )
            else:
                self.get_logger().error(
                    "Conflicting VIEW-CHANGE messages detected from "
                    "the same sender: "
                    f"sender={message.sender_id}, "
                    f"new_view={message.new_view}."
                )

            return




        messages_for_view[
            message.sender_id
        ] = message

        sender_ids = sorted(messages_for_view)

        self.get_logger().info(
            "Accepted VIEW-CHANGE: "
            f"sender={message.sender_id}, "
            f"new_view={message.new_view}, "
            f"has_prepared_certificate="
            f"{message.has_prepared_certificate}, "
            f"view_change_count={len(sender_ids)}, "
            f"threshold={self.view_change_threshold}, "
            f"senders={sender_ids}"
        )


        self._maybe_publish_new_view(message.new_view)



    def request_callback(self, message: PBFTMessage) -> None:
        """Validate and cache a client REQUEST on every replica."""
        was_already_cached = (
            message.request_id in self.cached_client_requests
        )

        instance = self._validate_and_cache_client_request(
            message
        )

        if instance is None:
            return

        # A repeated client transmission must not indefinitely reset
        # the progress timeout.
        if not was_already_cached:
            self._arm_progress_timeout(
                message.request_id,
                reason="new valid client REQUEST cached",
            )

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

        if self._is_skip_pre_prepare_byzantine():
            self.processed_request_ids.add(
                message.request_id
            )

            self.phase = "FAULTY_PRIMARY"

            self._publish_status(
                "Byzantine primary accepted the REQUEST but "
                "intentionally skipped PRE-PREPARE."
            )

            self.get_logger().warning(
                "SKIP-PRE-PREPARE BYZANTINE BEHAVIOR: "
                f"primary={self.node_id}, "
                f"view={self.current_view}, "
                f"request_id={message.request_id}. "
                "No PRE-PREPARE will be published."
            )
            return
        


        self._process_cached_request_as_primary(
            message.request_id
        )




    def _cancel_progress_timeout(
        self,
        reason: str,
    ) -> None:
        """Cancel the active timeout after successful completion."""
        if self.progress_timeout_timer is None:
            return

        request_id = self.progress_timeout_request_id
        view = self.progress_timeout_view

        self._clear_progress_timeout_timer()

        self.progress_timeout_request_id = None
        self.progress_timeout_view = None

        self.get_logger().info(
            "Progress timeout cancelled: "
            f"request_id={request_id}, "
            f"view={view}, "
            f"reason={reason}"
        )



    def _progress_timeout_callback(
        self,
    ) -> None:
        """Initiate VIEW-CHANGE when PBFT progress has stalled."""
        request_id = self.progress_timeout_request_id
        monitored_view = self.progress_timeout_view

        self._clear_progress_timeout_timer()

        self.progress_timeout_request_id = None
        self.progress_timeout_view = None

        if request_id is None or monitored_view is None:
            self.get_logger().warning(
                "Progress timeout fired without an active request."
            )
            return

        if monitored_view != self.current_view:
            self.get_logger().info(
                "Ignoring obsolete progress timeout: "
                f"monitored_view={monitored_view}, "
                f"current_view={self.current_view}, "
                f"request_id={request_id}"
            )
            return

        if (
            self.current_key is not None
            and self.current_key in self.committed_instances
        ):
            self.get_logger().info(
                "Ignoring progress timeout because the request "
                "is already committed: "
                f"request_id={request_id}, "
                f"view={self.current_view}"
            )
            return

        target_view = self.current_view + 1

        self.get_logger().warning(
            "PBFT PROGRESS TIMEOUT: "
            f"request_id={request_id}, "
            f"current_view={self.current_view}, "
            f"target_view={target_view}, "
            f"phase={self.phase}"
        )

        self._initiate_view_change(
            target_view,
            reason=(
                "progress timeout for "
                f"request_id={request_id}"
            ),
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

            self._arm_progress_timeout(
                message.request_id,
                reason="valid PRE-PREPARE accepted"
            )

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


    def _clear_progress_timeout_timer(
        self,
    ) -> None:
        """Cancel and destroy the current progress timer."""
        timer = self.progress_timeout_timer

        self.progress_timeout_timer = None

        if timer is not None:
            timer.cancel()
            self.destroy_timer(timer)

    def _arm_progress_timeout(
        self,
        request_id: str,
        reason: str,
    ) -> None:
        """Start or reset the progress timeout for one request."""
        if not self.enable_progress_timeout:
            return

        if not request_id:
            self.get_logger().error(
                "Cannot arm progress timeout for an empty request_id."
            )
            return

        self._clear_progress_timeout_timer()

        self.progress_timeout_request_id = request_id
        self.progress_timeout_view = self.current_view

        self.progress_timeout_timer = self.create_timer(
            self.progress_timeout_sec,
            self._progress_timeout_callback,
        )

        self.get_logger().info(
            "Progress timeout armed: "
            f"request_id={request_id}, "
            f"view={self.current_view}, "
            f"timeout_sec={self.progress_timeout_sec:.3f}, "
            f"reason={reason}"
        )



    def _manual_view_change_timer_callback(
        self,
    ) -> None:
        """Invoke VIEW-CHANGE through the controlled test hook."""
        if self.manual_view_change_timer is not None:
            self.manual_view_change_timer.cancel()
            self.manual_view_change_timer = None

        self._initiate_view_change(
            self.manual_view_change_target,
            reason="manual test trigger",
        )



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

        self._arm_progress_timeout(
            instance.request_id,
            reason="replica entered PREPARED",
        )

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

        self._cancel_progress_timeout(
            reason="request reached COMMITTED",
        )


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
