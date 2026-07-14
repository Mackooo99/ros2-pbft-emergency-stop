"""VIEW-CHANGE / NEW-VIEW handling.

This is the largest and most delicate piece of the protocol: forming
and validating a VIEW-CHANGE certificate, building/validating
NEW-VIEW, re-broadcasting a recovery PRE-PREPARE for any request that
must be carried over into the new view, and the manual test-only
trigger that lets a scenario force a view change on demand.
"""

from pbft_emergency_stop_interfaces.msg import NewView, PBFTMessage, ViewChange

from ..protocol import compute_request_digest
from .types import MessageKey, PBFTInstance


class ViewChangeMixin:
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




        for key, timer in list(
            self.recovery_pre_prepare_timers.items()
        ):
            if key[0] < new_view:
                timer.cancel()
                self.destroy_timer(timer)

                self.recovery_pre_prepare_timers.pop(
                    key,
                    None,
                )



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




    def _schedule_recovery_pre_prepare(
        self,
        key: MessageKey,
    ) -> None:
        """Schedule PRE-PREPARE after the new primary installs NEW-VIEW."""
        view, sequence_number = key

        if view != self.current_view:
            self.get_logger().warning(
                "Recovery PRE-PREPARE was not scheduled because "
                "the instance does not belong to the current view: "
                f"key={key}, current_view={self.current_view}."
            )
            return

        if self.node_id != self.primary_id:
            return

        if key in self.recovery_pre_prepare_sent:
            return

        if key in self.recovery_pre_prepare_timers:
            return

        if key not in self.instances:
            self.get_logger().error(
                "Recovery PRE-PREPARE was not scheduled because "
                f"the selected instance does not exist: key={key}."
            )
            return

        if self._is_skip_pre_prepare_byzantine():
            self.phase = "FAULTY_PRIMARY"

            self._publish_status(
                "The new Byzantine primary intentionally skipped "
                "the recovery PRE-PREPARE."
            )

            self.get_logger().warning(
                "SKIP-PRE-PREPARE BYZANTINE BEHAVIOR: "
                f"new_primary={self.node_id}, "
                f"view={view}, "
                f"sequence={sequence_number}. "
                "No recovery PRE-PREPARE will be published."
            )
            return

        timer = self.create_timer(
            self.new_view_pre_prepare_delay_sec,
            lambda scheduled_key=key: (
                self._publish_recovery_pre_prepare(
                    scheduled_key
                )
            ),
        )

        self.recovery_pre_prepare_timers[
            key
        ] = timer

        self.get_logger().info(
            "Recovery PRE-PREPARE scheduled by the new primary: "
            f"node_id={self.node_id}, "
            f"view={view}, "
            f"sequence={sequence_number}, "
            f"delay_sec="
            f"{self.new_view_pre_prepare_delay_sec:.3f}"
        )



    def _publish_recovery_pre_prepare(
        self,
        key: MessageKey,
    ) -> None:
        """Publish the request selected by an accepted NEW-VIEW."""
        timer = self.recovery_pre_prepare_timers.pop(
            key,
            None,
        )

        if timer is not None:
            timer.cancel()
            self.destroy_timer(timer)

        if key in self.recovery_pre_prepare_sent:
            return

        view, sequence_number = key

        if view != self.current_view:
            self.get_logger().warning(
                "Obsolete recovery PRE-PREPARE was cancelled: "
                f"key={key}, "
                f"current_view={self.current_view}."
            )
            return

        if self.node_id != self.primary_id:
            self.get_logger().warning(
                "Recovery PRE-PREPARE was cancelled because this "
                "replica is no longer the active primary: "
                f"node_id={self.node_id}, "
                f"primary_id={self.primary_id}, "
                f"view={self.current_view}."
            )
            return

        instance = self.instances.get(key)

        if instance is None:
            self.get_logger().error(
                "Recovery PRE-PREPARE could not be published "
                f"because the selected instance is missing: key={key}."
            )
            return

        pre_prepare = PBFTMessage()

        pre_prepare.stamp = (
            self.get_clock().now().to_msg()
        )
        pre_prepare.message_type = (
            PBFTMessage.PRE_PREPARE
        )
        pre_prepare.sender_id = self.node_id
        pre_prepare.recipient_id = -1
        pre_prepare.view = view
        pre_prepare.sequence_number = (
            sequence_number
        )
        pre_prepare.request_id = (
            instance.request_id
        )
        pre_prepare.request_digest = (
            instance.request_digest
        )
        pre_prepare.emergency_stop = (
            instance.emergency_stop
        )

        # Mark before publication so a repeated callback cannot
        # produce a second PRE-PREPARE for the same instance.
        self.recovery_pre_prepare_sent.add(key)

        self.current_key = key
        self.phase = "PRE_PREPARED"

        self._publish_status(
            "New primary published recovery PRE-PREPARE "
            "for the request selected by NEW-VIEW."
        )

        self.pre_prepare_publisher.publish(
            pre_prepare
        )

        self._arm_progress_timeout(
            instance.request_id,
            reason=(
                "new primary published recovery PRE-PREPARE"
            ),
        )

        self.get_logger().warning(
            "Published recovery PRE-PREPARE: "
            f"sender={pre_prepare.sender_id}, "
            f"view={pre_prepare.view}, "
            f"sequence={pre_prepare.sequence_number}, "
            f"request_id={pre_prepare.request_id}, "
            f"digest={pre_prepare.request_digest[:12]}..."
        )




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

        # The recovered request is now active in the new view.
        # Start a fresh timer so another stalled primary can cause
        # a later view change.
        self._arm_progress_timeout(
            selected_instance.request_id,
            reason="valid NEW-VIEW installed",
        )

        if self.node_id == self.primary_id:
            self._schedule_recovery_pre_prepare(
                new_key
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



