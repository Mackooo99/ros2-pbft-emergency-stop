"""Byzantine fault-injection behavior for the PBFT replica.

Every method here is a small, pure query or transform: the "_is_*"
methods answer "is this replica configured to misbehave in way X
right now", and the "_outgoing_*" methods let correct-looking
outgoing fields be corrupted for the byzantine faults that operate
on message content rather than message timing.

This mixin is combined with the other replica mixins in
``pbft_replica.PBFTReplica``. It expects ``self.is_byzantine``,
``self.byzantine_behavior``, ``self.node_id`` and
``self.replica_count`` to already be set by ``PBFTReplica.__init__``.
"""

from pbft_emergency_stop_interfaces.msg import PBFTMessage

from ..protocol import compute_request_digest
from .types import MessageKey


class ByzantineBehaviorMixin:
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