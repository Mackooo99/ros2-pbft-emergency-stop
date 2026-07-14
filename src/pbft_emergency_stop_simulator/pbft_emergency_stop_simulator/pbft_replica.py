"""PBFT replica implementation through the COMMIT phase.

This module wires together the PBFT replica out of focused mixins,
one per protocol phase (see ``replica/``). All state lives here in
``PBFTReplica.__init__``; the mixins only add behavior on top of it.
Splitting it this way keeps each phase independently readable and
testable while every mixin still shares the same ``self`` state, so
no protocol logic changes as part of this reorganization.
"""

from collections import defaultdict

import rclpy
from rclpy.node import Node

from pbft_emergency_stop_interfaces.msg import (
    NewView,
    PBFTMessage,
    ReplicaStatus,
    ViewChange,
)

from .replica.byzantine import ByzantineBehaviorMixin
from .replica.commit import CommitMixin
from .replica.pre_prepare import PrePrepareMixin
from .replica.prepare import PrepareMixin
from .replica.progress_timeout import ProgressTimeoutMixin
from .replica.qos import create_pbft_qos, create_status_qos
from .replica.request_handling import RequestHandlingMixin
from .replica.status import StatusMixin
from .replica.types import MessageKey, PBFTInstance
from .replica.view_change import ViewChangeMixin


class PBFTReplica(
    ByzantineBehaviorMixin,
    RequestHandlingMixin,
    ViewChangeMixin,
    ProgressTimeoutMixin,
    PrePrepareMixin,
    PrepareMixin,
    CommitMixin,
    StatusMixin,
    Node,
):
    """PBFT replica supporting REQUEST through COMMIT.

    Behavior is composed from mixins (one per protocol phase); see
    the module docstrings under ``replica/`` for what each one
    covers. This class itself only owns startup: parameter
    declaration/validation, publishers/subscriptions, and initial
    state.
    """

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
        self.declare_parameter("new_view_pre_prepare_delay_sec", 0.5)
        self.declare_parameter("status_publish_period_sec", 1.0)
        self.declare_parameter("protocol_qos_depth", 20)
        self.declare_parameter("status_qos_depth", 1)



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
        self.new_view_pre_prepare_delay_sec = float(
            self.get_parameter("new_view_pre_prepare_delay_sec").value
        )
        self.status_publish_period_sec = float(
            self.get_parameter("status_publish_period_sec").value
        )
        self.protocol_qos_depth = int(
            self.get_parameter("protocol_qos_depth").value
        )
        self.status_qos_depth = int(
            self.get_parameter("status_qos_depth").value
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
        
        if self.new_view_pre_prepare_delay_sec <= 0.0:
            raise ValueError(
                "new_view_pre_prepare_delay_sec must be positive."
            )

        if self.status_publish_period_sec <= 0.0:
            raise ValueError(
                "status_publish_period_sec must be positive."
            )

        if self.protocol_qos_depth <= 0:
            raise ValueError(
                "protocol_qos_depth must be positive."
            )

        if self.status_qos_depth <= 0:
            raise ValueError(
                "status_qos_depth must be positive."
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



        # PBFT instances for which the new primary already published
        # the recovery PRE-PREPARE after accepting NEW-VIEW.
        self.recovery_pre_prepare_sent: set[
            MessageKey
        ] = set()

        # One-shot timers used to delay recovery PRE-PREPARE until
        # the replicas have received and activated NEW-VIEW.
        self.recovery_pre_prepare_timers: dict[
            MessageKey,
            object,
        ] = {}

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
            create_pbft_qos(self.protocol_qos_depth),
        )

        self.prepare_publisher = self.create_publisher(
            PBFTMessage,
            "/pbft/prepare",
            create_pbft_qos(self.protocol_qos_depth),
        )

        self.commit_publisher = self.create_publisher(
            PBFTMessage,
            "/pbft/commit",
            create_pbft_qos(self.protocol_qos_depth),
        )
        
        self.status_publisher = self.create_publisher(
            ReplicaStatus,
            "/pbft/status",
            create_status_qos(self.status_qos_depth),
	    )

        self.view_change_publisher = self.create_publisher(
            ViewChange,
            "/pbft/view_change",
            create_pbft_qos(self.protocol_qos_depth),
        )

        self.new_view_publisher = self.create_publisher(
            NewView,
            "/pbft/new_view",
            create_pbft_qos(self.protocol_qos_depth),
        )


        # Subscriptions.
        self.request_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/request",
            self.request_callback,
            create_pbft_qos(self.protocol_qos_depth),
        )

        self.pre_prepare_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/pre_prepare",
            self.pre_prepare_callback,
            create_pbft_qos(self.protocol_qos_depth),
        )

        self.prepare_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/prepare",
            self.prepare_callback,
            create_pbft_qos(self.protocol_qos_depth),
        )

        self.commit_subscription = self.create_subscription(
            PBFTMessage,
            "/pbft/commit",
            self.commit_callback,
            create_pbft_qos(self.protocol_qos_depth),
        )

        self.view_change_subscription = self.create_subscription(
            ViewChange,
            "/pbft/view_change",
            self.view_change_callback,
            create_pbft_qos(self.protocol_qos_depth),
        )


        self.new_view_subscription = self.create_subscription(
            NewView,
            "/pbft/new_view",
            self.new_view_callback,
            create_pbft_qos(self.protocol_qos_depth),
        )

        
        self.status_timer = self.create_timer(
            self.status_publish_period_sec,
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
            f"{self.progress_timeout_sec}, "
            f"new_view_pre_prepare_delay_sec="
            f"{self.new_view_pre_prepare_delay_sec}, "
            f"status_publish_period_sec="
            f"{self.status_publish_period_sec}, "
            f"protocol_qos_depth={self.protocol_qos_depth}, "
            f"status_qos_depth={self.status_qos_depth}"
        )
        
        self._publish_status(self.status_detail)
        
    
    


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