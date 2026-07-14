"""PRE-PREPARE handling (backup side).

Validates an incoming PRE-PREPARE from the primary and, if accepted,
moves the instance into the PREPARE phase.
"""

from pbft_emergency_stop_interfaces.msg import PBFTMessage

from ..protocol import compute_request_digest
from .types import PBFTInstance


class PrePrepareMixin:
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