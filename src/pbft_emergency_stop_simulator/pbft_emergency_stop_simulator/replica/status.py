"""Status publishing and static configuration checks for the replica.

Kept separate from protocol handling because it has no PBFT-phase
logic of its own: it only reports current state (``_publish_status``)
and validates the static n/f/view/primary configuration once at
startup (``_validate_configuration``, ``_primary_for_view``).
"""

from pbft_emergency_stop_interfaces.msg import ReplicaStatus


class StatusMixin:
    def _publish_status(self, detail: str = "") -> None:
        """Publish the current local state of this replica."""
        if detail:
            self.status_detail = detail

        status = ReplicaStatus()

        status.stamp = self.get_clock().now().to_msg()
        status.node_id = self.node_id
        status.view = self.current_view

        if self.current_key is None:
            status.sequence_number = 0
            status.request_id = ""
            status.request_digest = ""

            prepare_count = 0
            commit_count = 0
            prepared = False
            committed = False
        else:
            key = self.current_key
            instance = self.instances.get(key)

            status.sequence_number = key[1]

            if instance is None:
                status.request_id = ""
                status.request_digest = ""
            else:
                status.request_id = instance.request_id
                status.request_digest = instance.request_digest

            prepare_count = len(
                self.prepare_senders.get(key, set())
            )
            commit_count = len(
                self.commit_senders.get(key, set())
            )
            prepared = key in self.prepared_instances
            committed = key in self.committed_instances

        status.phase = self.phase
        status.prepare_count = prepare_count
        status.commit_count = commit_count
        status.prepared = prepared
        status.committed = committed
        status.emergency_stop = self.emergency_stop
        status.is_byzantine = self.is_byzantine
        status.detail = self.status_detail

        self.status_publisher.publish(status)

    
    
    def _primary_for_view(self, view: int) -> int:
        """Return the primary replica assigned to the given view."""
        if view < 0:
            raise ValueError(
                "PBFT view must be non-negative."
            )

        return view % self.replica_count
    
    
    
    def _validate_configuration(self) -> None:
        """Validate replica identity and the supported PBFT configuration."""
        if self.max_faulty < 0:
            raise ValueError(
                "max_faulty must be non-negative."
            )

        expected_replica_count = 3 * self.max_faulty + 1

        if self.replica_count != expected_replica_count:
            raise ValueError(
                "Invalid PBFT configuration: "
                f"n={self.replica_count}, "
                f"f={self.max_faulty}. "
                "This simulator currently requires "
                f"n = 3f + 1 = {expected_replica_count}."
            )

        if not 0 <= self.node_id < self.replica_count:
            raise ValueError(
                f"node_id={self.node_id} is outside the valid range "
                f"0..{self.replica_count - 1}."
            )

        if self.current_view < 0:
            raise ValueError(
                "current_view must be non-negative."
            )

        if not (
            0
            <= self.configured_primary_id
            < self.replica_count
        ):
            raise ValueError(
                "Configured primary_id="
                f"{self.configured_primary_id} is outside the valid "
                f"range 0..{self.replica_count - 1}."
            )

        expected_primary_id = self._primary_for_view(
            self.current_view
        )

        if self.configured_primary_id != expected_primary_id:
            raise ValueError(
                "Invalid initial primary configuration: "
                f"current_view={self.current_view}, "
                f"configured_primary_id="
                f"{self.configured_primary_id}, "
                f"expected_primary_id="
                f"{expected_primary_id}. "
                "The primary must satisfy "
                "primary_id = current_view % replica_count."
            )




