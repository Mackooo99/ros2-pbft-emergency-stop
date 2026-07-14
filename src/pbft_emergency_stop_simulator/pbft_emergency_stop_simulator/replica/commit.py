"""COMMIT phase: sending, receiving, buffering, and quorum evaluation.

Mirrors the structure of ``prepare.py`` one phase later. Early
COMMIT messages that arrive before this replica is locally PREPARED
are buffered (not dropped) and re-evaluated once PREPARED is reached,
which is what lets ``pbft_early_commit_scenario`` demonstrate a real
(but ultimately safe) race instead of a rejected message.
"""

from pbft_emergency_stop_interfaces.msg import PBFTMessage

from ..protocol import compute_request_digest
from .types import MessageKey


class CommitMixin:
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


