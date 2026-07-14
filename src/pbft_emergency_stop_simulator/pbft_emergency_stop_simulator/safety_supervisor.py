"""Fail-safe supervisor for PBFT emergency-stop decisions."""

from __future__ import annotations

from dataclasses import dataclass

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    QoSProfile,
    ReliabilityPolicy,
)
from rclpy.time import Time
from std_msgs.msg import Bool, String

from pbft_emergency_stop_interfaces.msg import PBFTDecision, PBFTMessage

from .protocol import compute_request_digest


@dataclass(frozen=True)
class ActiveRequest:
    request_id: str
    request_digest: str
    emergency_stop: bool
    initial_view: int
    received_time_ns: int


def protocol_qos() -> QoSProfile:
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=20,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.VOLATILE,
    )


def decision_qos() -> QoSProfile:
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=20,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )


def safety_qos() -> QoSProfile:
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=1,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )


class SafetySupervisor(Node):
    """Apply valid PBFT decisions and latch fail-safe stopping on faults."""

    def __init__(self) -> None:
        super().__init__("safety_supervisor")

        self.declare_parameter("replica_count", 4)
        self.declare_parameter("max_faulty", 1)
        self.declare_parameter("decision_timeout_sec", 8.0)
        self.declare_parameter("allow_confirmed_release", False)
        self.declare_parameter("heartbeat_period_sec", 0.5)

        self.replica_count = int(self.get_parameter("replica_count").value)
        self.max_faulty = int(self.get_parameter("max_faulty").value)
        self.decision_timeout_sec = float(
            self.get_parameter("decision_timeout_sec").value
        )
        self.allow_confirmed_release = bool(
            self.get_parameter("allow_confirmed_release").value
        )
        self.heartbeat_period_sec = float(
            self.get_parameter("heartbeat_period_sec").value
        )

        self._validate_configuration()
        self.required_confirmations = 2 * self.max_faulty + 1

        self.active_request: ActiveRequest | None = None
        self.timeout_timer = None
        self.decision_processed = False
        self.fail_safe_latched = False
        self.current_command = True
        self.current_state = "INITIALIZING"

        self.command_publisher = self.create_publisher(
            Bool, "/vehicle/emergency_stop", safety_qos()
        )
        self.state_publisher = self.create_publisher(
            String, "/safety/state", safety_qos()
        )

        self.request_subscription = self.create_subscription(
            PBFTMessage, "/pbft/request", self.request_callback, protocol_qos()
        )
        self.decision_subscription = self.create_subscription(
            PBFTDecision, "/pbft/decision", self.decision_callback, decision_qos()
        )

        self.heartbeat_timer = self.create_timer(
            self.heartbeat_period_sec, self.heartbeat_callback
        )

        self._publish_output(True, "safe initial state", force_log=True)
        self._set_state(
            "SAFE_INITIAL_STOP",
            "Supervisor starts stopped until a valid distributed decision is received.",
        )

        self.get_logger().info(
            "Safety supervisor initialized: "
            f"n={self.replica_count}, f={self.max_faulty}, "
            f"required_confirmations={self.required_confirmations}, "
            f"decision_timeout_sec={self.decision_timeout_sec:.3f}, "
            f"allow_confirmed_release={self.allow_confirmed_release}"
        )

    def _validate_configuration(self) -> None:
        if self.replica_count != 3 * self.max_faulty + 1:
            raise ValueError("PBFT requires replica_count = 3f + 1.")
        if self.decision_timeout_sec <= 0.0:
            raise ValueError("decision_timeout_sec must be positive.")
        if self.heartbeat_period_sec <= 0.0:
            raise ValueError("heartbeat_period_sec must be positive.")

    def request_callback(self, message: PBFTMessage) -> None:
        if message.message_type != PBFTMessage.REQUEST:
            return

        error = self._validate_request(message)
        if error:
            self._activate_fail_safe(f"invalid client request: {error}")
            return

        candidate = ActiveRequest(
            request_id=message.request_id,
            request_digest=message.request_digest,
            emergency_stop=message.emergency_stop,
            initial_view=message.view,
            received_time_ns=self.get_clock().now().nanoseconds,
        )

        if self.active_request is not None and not self.decision_processed:
            active = self.active_request
            same_request = (
                candidate.request_id == active.request_id
                and candidate.request_digest == active.request_digest
                and candidate.emergency_stop == active.emergency_stop
                and candidate.initial_view == active.initial_view
            )
            if same_request:
                self.get_logger().warning(
                    f"Duplicate active REQUEST ignored: {candidate.request_id}."
                )
                return
            self._activate_fail_safe("concurrent or conflicting client requests")
            return

        self.active_request = candidate
        self.decision_processed = False
        self._cancel_timeout()
        self._publish_output(True, "PBFT decision pending")
        self._set_state(
            "WAITING_FOR_DECISION",
            f"Waiting for request {candidate.request_id}.",
        )

        self.timeout_timer = self.create_timer(
            self.decision_timeout_sec, self.decision_timeout_callback
        )

        self.get_logger().info(
            "Safety deadline armed: "
            f"request_id={candidate.request_id}, "
            f"requested_emergency_stop={candidate.emergency_stop}, "
            f"timeout_sec={self.decision_timeout_sec:.3f}"
        )

    def _validate_request(self, message: PBFTMessage) -> str | None:
        if message.sender_id != -1:
            return "sender_id must be -1"
        if not 0 <= message.recipient_id < self.replica_count:
            return "invalid recipient_id"
        if message.sequence_number != 0:
            return "client sequence_number must be zero"
        if not message.request_id:
            return "empty request_id"
        expected = compute_request_digest(message.request_id, message.emergency_stop)
        if message.request_digest != expected:
            return "digest mismatch"
        return None

    def decision_callback(self, message: PBFTDecision) -> None:
        request = self.active_request
        if request is None or message.request_id != request.request_id:
            return

        if Time.from_msg(message.stamp).nanoseconds < request.received_time_ns:
            return

        error = self._validate_decision(message, request)
        if error:
            self.get_logger().error(
                f"Rejected invalid matching PBFT decision: {error}."
            )
            self._activate_fail_safe(f"invalid matching PBFT decision: {error}")
            return

        if self.decision_processed:
            return

        self.decision_processed = True
        self._cancel_timeout()

        if message.emergency_stop:
            self._publish_output(True, "confirmed PBFT stop decision")
            self._set_state("CONFIRMED_STOP", "PBFT quorum confirmed stop.")
            self.get_logger().warning(
                "CONFIRMED EMERGENCY STOP APPLIED: "
                f"request_id={message.request_id}, view={message.view}, "
                f"sequence={message.sequence_number}, "
                f"confirming_replicas={sorted(message.confirming_replicas)}"
            )
            return

        if self.fail_safe_latched or not self.allow_confirmed_release:
            self._publish_output(True, "release blocked by safety policy")
            self._set_state("RELEASE_BLOCKED", "Automatic release is prohibited.")
            return

        self._publish_output(False, "confirmed PBFT release decision")
        self._set_state("CONFIRMED_RELEASE", "PBFT quorum confirmed release.")

    def _validate_decision(
        self, message: PBFTDecision, request: ActiveRequest
    ) -> str | None:
        if not message.committed:
            return "decision is not committed"
        if message.view < request.initial_view:
            return "decision view is stale"
        if message.sequence_number <= 0:
            return "invalid sequence number"
        if message.request_digest != request.request_digest:
            return "digest differs from active request"
        if message.emergency_stop != request.emergency_stop:
            return "decision value differs from request"

        replicas = list(message.confirming_replicas)
        unique = set(replicas)
        if len(unique) != len(replicas):
            return "duplicate confirming replicas"
        if any(node_id < 0 or node_id >= self.replica_count for node_id in unique):
            return "invalid confirming replica"
        if message.confirmation_count != len(unique):
            return "confirmation_count mismatch"
        if message.required_confirmations != self.required_confirmations:
            return "required_confirmations mismatch"
        if len(unique) < self.required_confirmations:
            return "insufficient confirmation quorum"
        return None

    def decision_timeout_callback(self) -> None:
        if self.active_request is None or self.decision_processed:
            self._cancel_timeout()
            return

        request_id = self.active_request.request_id
        self._cancel_timeout()
        self.get_logger().error(
            "PBFT DECISION TIMEOUT: "
            f"request_id={request_id}, timeout_sec={self.decision_timeout_sec:.3f}"
        )
        self._activate_fail_safe(
            "no valid PBFT decision arrived before the safety deadline"
        )

    def _activate_fail_safe(self, reason: str) -> None:
        self.fail_safe_latched = True
        self._publish_output(True, f"FAIL-SAFE: {reason}", force_log=True)
        self._set_state("FAIL_SAFE_STOP", reason)
        self.get_logger().error(
            f"FAIL-SAFE EMERGENCY STOP LATCHED: reason={reason}"
        )

    def _publish_output(
        self, enabled: bool, reason: str, force_log: bool = False
    ) -> None:
        changed = enabled != self.current_command
        self.current_command = enabled
        message = Bool()
        message.data = enabled
        self.command_publisher.publish(message)
        if changed or force_log:
            self.get_logger().warning(
                f"SAFETY OUTPUT UPDATED: emergency_stop={enabled}, reason={reason}"
            )

    def _set_state(self, state: str, reason: str) -> None:
        changed = state != self.current_state
        self.current_state = state
        message = String()
        message.data = state
        self.state_publisher.publish(message)
        if changed:
            self.get_logger().info(
                f"SAFETY STATE CHANGED: state={state}, reason={reason}"
            )

    def heartbeat_callback(self) -> None:
        command = Bool()
        command.data = self.current_command
        self.command_publisher.publish(command)
        state = String()
        state.data = self.current_state
        self.state_publisher.publish(state)

    def _cancel_timeout(self) -> None:
        if self.timeout_timer is not None:
            self.timeout_timer.cancel()
            self.timeout_timer = None


def main(args=None) -> None:
    rclpy.init(args=args)
    node = SafetySupervisor()
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
