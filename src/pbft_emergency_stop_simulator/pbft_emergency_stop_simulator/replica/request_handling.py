"""Client REQUEST intake and caching.

Handles the very first PBFT step: validating an incoming client
REQUEST, caching it (so a future primary can resume it after a view
change), and -- if this replica is currently primary -- assigning it
a sequence number and broadcasting PRE-PREPARE.
"""

from pbft_emergency_stop_interfaces.msg import PBFTMessage

from ..protocol import compute_request_digest
from .types import PBFTInstance


class RequestHandlingMixin:
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




