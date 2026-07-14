"""PREPARE phase: sending, receiving, and quorum evaluation.

Also hosts ``_send_early_commit``, the byzantine helper that
publishes COMMIT before this replica has locally reached PREPARED
(used only when ``byzantine_behavior == "early_commit"``); it lives
here because it is triggered from the same place normal PREPARE
would be sent.
"""

from pbft_emergency_stop_interfaces.msg import PBFTMessage

from ..protocol import compute_request_digest
from .types import MessageKey


class PrepareMixin:
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

