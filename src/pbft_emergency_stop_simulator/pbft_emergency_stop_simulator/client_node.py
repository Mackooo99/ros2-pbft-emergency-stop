"""Client node that sends a request and waits for a PBFT decision."""

from uuid import uuid4

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    QoSProfile,
    ReliabilityPolicy,
)
from rclpy.time import Time

from pbft_emergency_stop_interfaces.msg import (
    PBFTDecision,
    PBFTMessage,
)

from .protocol import compute_request_digest


def create_pbft_qos() -> QoSProfile:
    """Create QoS for PBFT protocol messages."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=10,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.VOLATILE,
    )


def create_decision_qos() -> QoSProfile:
    """Create QoS compatible with the decision publisher."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=20,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )


class PBFTClient(Node):
    """Send one request and wait for its confirmed PBFT decision."""

    def __init__(self) -> None:
        super().__init__("client_node")

        self.declare_parameter("primary_id", 0)
        self.declare_parameter("current_view", 0)
        self.declare_parameter("emergency_stop", True)
        self.declare_parameter("request_id", "")
        self.declare_parameter("publish_delay_sec", 1.0)
        self.declare_parameter("decision_timeout_sec", 15.0)

        self.primary_id = int(
            self.get_parameter("primary_id").value
        )
        self.current_view = int(
            self.get_parameter("current_view").value
        )
        self.emergency_stop = bool(
            self.get_parameter("emergency_stop").value
        )

        configured_request_id = str(
            self.get_parameter("request_id").value
        )

        if configured_request_id:
            self.request_id = configured_request_id
        else:
            self.request_id = f"estop-{uuid4().hex[:8]}"

        self.request_digest = compute_request_digest(
            self.request_id,
            self.emergency_stop,
        )

        self.publish_delay_sec = float(
            self.get_parameter("publish_delay_sec").value
        )
        self.decision_timeout_sec = float(
            self.get_parameter("decision_timeout_sec").value
        )

        if self.publish_delay_sec < 0.0:
            raise ValueError(
                "publish_delay_sec must be non-negative."
            )

        if self.decision_timeout_sec <= 0.0:
            raise ValueError(
                "decision_timeout_sec must be positive."
            )

        self.request_sent = False
        self.decision_received = False

        self.request_sent_time_ns: int | None = None

        self.decision_timeout_timer = None
        self.shutdown_timer = None

        self.publisher = self.create_publisher(
            PBFTMessage,
            "/pbft/request",
            create_pbft_qos(),
        )

        self.decision_subscription = self.create_subscription(
            PBFTDecision,
            "/pbft/decision",
            self.decision_callback,
            create_decision_qos(),
        )

        # Delay the first publication to allow DDS discovery.
        self.publish_timer = self.create_timer(
            self.publish_delay_sec,
            self.publish_request,
        )

        self.get_logger().info(
            "PBFT client started. "
            f"primary={self.primary_id}, "
            f"initial_view={self.current_view}, "
            f"request_id={self.request_id}, "
            f"decision_timeout_sec={self.decision_timeout_sec:.3f}"
        )

    def publish_request(self) -> None:
        """Construct and publish one emergency-stop request."""
        if self.request_sent:
            return

        now = self.get_clock().now()

        message = PBFTMessage()

        message.stamp = now.to_msg()
        message.message_type = PBFTMessage.REQUEST

        # -1 represents the external client.
        message.sender_id = -1
        message.recipient_id = self.primary_id

        message.view = self.current_view

        # The primary assigns the PBFT sequence number.
        message.sequence_number = 0

        message.request_id = self.request_id
        message.request_digest = self.request_digest
        message.emergency_stop = self.emergency_stop

        # Mark the request before publishing so a fast decision callback
        # can recognize that the request is active.
        self.request_sent = True
        self.request_sent_time_ns = now.nanoseconds

        self.publisher.publish(message)
        self.publish_timer.cancel()

        self.get_logger().info(
            "Published REQUEST: "
            f"request_id={message.request_id}, "
            f"recipient={message.recipient_id}, "
            f"initial_view={message.view}, "
            f"emergency_stop={message.emergency_stop}, "
            f"digest={message.request_digest[:12]}..."
        )

        self.decision_timeout_timer = self.create_timer(
            self.decision_timeout_sec,
            self.decision_timeout_callback,
        )

        self.get_logger().info(
            "Waiting for confirmed decision on /pbft/decision: "
            f"request_id={self.request_id}"
        )

    def decision_callback(
        self,
        message: PBFTDecision,
    ) -> None:
        """Validate a confirmed decision for this client's request."""
        if self.decision_received:
            return

        if not self.request_sent:
            self.get_logger().warning(
                "Ignored PBFT decision received before the client "
                "published its request."
            )
            return

        if message.request_id != self.request_id:
            return

        if self.request_sent_time_ns is not None:
            decision_time_ns = Time.from_msg(
                message.stamp
            ).nanoseconds

            if decision_time_ns < self.request_sent_time_ns:
                self.get_logger().warning(
                    "Ignored stale PBFT decision published before "
                    "the current request: "
                    f"request_id={message.request_id}."
                )
                return

        if not message.committed:
            self.get_logger().warning(
                "Ignored non-committed PBFT decision: "
                f"request_id={message.request_id}."
            )
            return

        if message.sequence_number <= 0:
            self.get_logger().warning(
                "Ignored PBFT decision with invalid sequence number: "
                f"sequence={message.sequence_number}."
            )
            return

        if message.request_digest != self.request_digest:
            self.get_logger().error(
                "Rejected PBFT decision because its digest does not "
                "match the submitted request: "
                f"request_id={message.request_id}, "
                f"received_digest={message.request_digest[:12]}..., "
                f"expected_digest={self.request_digest[:12]}..."
            )
            return

        if message.emergency_stop != self.emergency_stop:
            self.get_logger().error(
                "Rejected PBFT decision because its value does not "
                "match the submitted request: "
                f"request_id={message.request_id}, "
                f"received_emergency_stop="
                f"{message.emergency_stop}, "
                f"expected_emergency_stop="
                f"{self.emergency_stop}."
            )
            return

        confirming_replicas = list(
            message.confirming_replicas
        )
        unique_confirming_replicas = sorted(
            set(confirming_replicas)
        )

        if (
            len(unique_confirming_replicas)
            != len(confirming_replicas)
        ):
            self.get_logger().error(
                "Rejected PBFT decision containing duplicate "
                "confirming replica IDs: "
                f"confirming_replicas={confirming_replicas}."
            )
            return

        if (
            message.confirmation_count
            != len(unique_confirming_replicas)
        ):
            self.get_logger().error(
                "Rejected PBFT decision with inconsistent "
                "confirmation_count: "
                f"confirmation_count="
                f"{message.confirmation_count}, "
                f"unique_replicas="
                f"{len(unique_confirming_replicas)}."
            )
            return

        if message.required_confirmations <= 0:
            self.get_logger().error(
                "Rejected PBFT decision with invalid "
                "required_confirmations."
            )
            return

        if (
            message.confirmation_count
            < message.required_confirmations
        ):
            self.get_logger().error(
                "Rejected PBFT decision without enough "
                "confirmations: "
                f"received={message.confirmation_count}, "
                f"required={message.required_confirmations}."
            )
            return

        self.decision_received = True

        if self.decision_timeout_timer is not None:
            self.decision_timeout_timer.cancel()

        self.get_logger().info(
            "=================================================="
        )
        self.get_logger().info(
            "PBFT REQUEST COMPLETED"
        )
        self.get_logger().info(
            f"request_id={message.request_id}"
        )
        self.get_logger().info(
            f"decision_view={message.view}"
        )
        self.get_logger().info(
            f"sequence_number={message.sequence_number}"
        )
        self.get_logger().info(
            f"emergency_stop={message.emergency_stop}"
        )
        self.get_logger().info(
            f"committed={message.committed}"
        )
        self.get_logger().info(
            "confirmations="
            f"{message.confirmation_count}/"
            f"{message.required_confirmations}"
        )
        self.get_logger().info(
            "confirming_replicas="
            f"{unique_confirming_replicas}"
        )
        self.get_logger().info(
            f"digest={message.request_digest[:12]}..."
        )
        self.get_logger().info(
            "=================================================="
        )

        self._schedule_shutdown(
            "Confirmed PBFT decision received. Client is stopping."
        )

    def decision_timeout_callback(self) -> None:
        """Stop waiting when no confirmed decision arrives in time."""
        if self.decision_received:
            return

        if self.decision_timeout_timer is not None:
            self.decision_timeout_timer.cancel()

        self.get_logger().error(
            "PBFT REQUEST TIMEOUT: "
            f"request_id={self.request_id}, "
            f"timeout_sec={self.decision_timeout_sec:.3f}. "
            "No confirmed decision was received."
        )

        self._schedule_shutdown(
            "Decision timeout expired. Client is stopping."
        )

    def _schedule_shutdown(
        self,
        reason: str,
    ) -> None:
        """Schedule a short delayed shutdown to flush final logs."""
        if self.shutdown_timer is not None:
            return

        self.get_logger().info(reason)

        self.shutdown_timer = self.create_timer(
            0.2,
            self.stop_client,
        )

    def stop_client(self) -> None:
        """Stop the client node."""
        if self.shutdown_timer is not None:
            self.shutdown_timer.cancel()

        if rclpy.ok():
            rclpy.shutdown()


def main(args=None) -> None:
    """Run the PBFT client node."""
    rclpy.init(args=args)

    node = PBFTClient()

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