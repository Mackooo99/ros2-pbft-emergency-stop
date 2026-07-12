"""Client node that sends an emergency-stop request to the PBFT primary."""

from uuid import uuid4

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    QoSProfile,
    ReliabilityPolicy,
)

from pbft_emergency_stop_interfaces.msg import PBFTMessage

from .protocol import compute_request_digest


def create_pbft_qos() -> QoSProfile:
    """Create the QoS profile used for PBFT protocol messages."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=10,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.VOLATILE,
    )


class PBFTClient(Node):
    """Send one emergency-stop request to the current primary replica."""

    def __init__(self) -> None:
        super().__init__("client_node")

        self.declare_parameter("primary_id", 0)
        self.declare_parameter("current_view", 0)
        self.declare_parameter("emergency_stop", True)
        self.declare_parameter("request_id", "")
        self.declare_parameter("publish_delay_sec", 1.0)

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

        publish_delay = float(
            self.get_parameter("publish_delay_sec").value
        )

        self.publisher = self.create_publisher(
            PBFTMessage,
            "/pbft/request",
            create_pbft_qos(),
        )

        self.request_sent = False
        self.shutdown_timer = None

        # The delay allows DDS discovery to finish before the first message.
        self.publish_timer = self.create_timer(
            publish_delay,
            self.publish_request,
        )

        self.get_logger().info(
            "PBFT client started. "
            f"Primary replica: {self.primary_id}, "
            f"view: {self.current_view}"
        )

    def publish_request(self) -> None:
        """Construct and publish one emergency-stop request."""
        if self.request_sent:
            return

        message = PBFTMessage()

        message.stamp = self.get_clock().now().to_msg()
        message.message_type = PBFTMessage.REQUEST

        # -1 represents the external client.
        message.sender_id = -1
        message.recipient_id = self.primary_id

        message.view = self.current_view

        # The primary assigns the real PBFT sequence number later.
        message.sequence_number = 0

        message.request_id = self.request_id
        message.emergency_stop = self.emergency_stop
        message.request_digest = compute_request_digest(
            message.request_id,
            message.emergency_stop,
        )

        self.publisher.publish(message)

        self.request_sent = True
        self.publish_timer.cancel()

        self.get_logger().info(
            "Published REQUEST: "
            f"request_id={message.request_id}, "
            f"recipient={message.recipient_id}, "
            f"view={message.view}, "
            f"emergency_stop={message.emergency_stop}, "
            f"digest={message.request_digest[:12]}..."
        )

        # Keep the process alive briefly so Fast DDS can transmit the message.
        self.shutdown_timer = self.create_timer(
            0.5,
            self.stop_client,
        )

    def stop_client(self) -> None:
        """Stop the client after the request has been transmitted."""
        if self.shutdown_timer is not None:
            self.shutdown_timer.cancel()

        self.get_logger().info("Request sent. Client is stopping.")
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
