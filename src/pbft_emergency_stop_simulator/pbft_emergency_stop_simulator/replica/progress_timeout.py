"""Progress-timeout watchdog.

Detects a PBFT instance that has stalled (no COMMIT quorum within
``progress_timeout_sec``) and triggers a VIEW-CHANGE. Only active
when ``enable_progress_timeout`` is true, since many fault-injection
scenarios intentionally want to observe a stalled instance without
the replica automatically recovering from it.
"""


class ProgressTimeoutMixin:
    def _cancel_progress_timeout(
        self,
        reason: str,
    ) -> None:
        """Cancel the active timeout after successful completion."""
        if self.progress_timeout_timer is None:
            return

        request_id = self.progress_timeout_request_id
        view = self.progress_timeout_view

        self._clear_progress_timeout_timer()

        self.progress_timeout_request_id = None
        self.progress_timeout_view = None

        self.get_logger().info(
            "Progress timeout cancelled: "
            f"request_id={request_id}, "
            f"view={view}, "
            f"reason={reason}"
        )



    def _progress_timeout_callback(
        self,
    ) -> None:
        """Initiate VIEW-CHANGE when PBFT progress has stalled."""
        request_id = self.progress_timeout_request_id
        monitored_view = self.progress_timeout_view

        self._clear_progress_timeout_timer()

        self.progress_timeout_request_id = None
        self.progress_timeout_view = None

        if request_id is None or monitored_view is None:
            self.get_logger().warning(
                "Progress timeout fired without an active request."
            )
            return

        if monitored_view != self.current_view:
            self.get_logger().info(
                "Ignoring obsolete progress timeout: "
                f"monitored_view={monitored_view}, "
                f"current_view={self.current_view}, "
                f"request_id={request_id}"
            )
            return

        if (
            self.current_key is not None
            and self.current_key in self.committed_instances
        ):
            self.get_logger().info(
                "Ignoring progress timeout because the request "
                "is already committed: "
                f"request_id={request_id}, "
                f"view={self.current_view}"
            )
            return

        target_view = self.current_view + 1

        self.get_logger().warning(
            "PBFT PROGRESS TIMEOUT: "
            f"request_id={request_id}, "
            f"current_view={self.current_view}, "
            f"target_view={target_view}, "
            f"phase={self.phase}"
        )

        self._initiate_view_change(
            target_view,
            reason=(
                "progress timeout for "
                f"request_id={request_id}"
            ),
        )




    def _clear_progress_timeout_timer(
        self,
    ) -> None:
        """Cancel and destroy the current progress timer."""
        timer = self.progress_timeout_timer

        self.progress_timeout_timer = None

        if timer is not None:
            timer.cancel()
            self.destroy_timer(timer)

    def _arm_progress_timeout(
        self,
        request_id: str,
        reason: str,
    ) -> None:
        """Start or reset the progress timeout for one request."""
        if not self.enable_progress_timeout:
            return

        if not request_id:
            self.get_logger().error(
                "Cannot arm progress timeout for an empty request_id."
            )
            return

        self._clear_progress_timeout_timer()

        self.progress_timeout_request_id = request_id
        self.progress_timeout_view = self.current_view

        self.progress_timeout_timer = self.create_timer(
            self.progress_timeout_sec,
            self._progress_timeout_callback,
        )

        self.get_logger().info(
            "Progress timeout armed: "
            f"request_id={request_id}, "
            f"view={self.current_view}, "
            f"timeout_sec={self.progress_timeout_sec:.3f}, "
            f"reason={reason}"
        )



