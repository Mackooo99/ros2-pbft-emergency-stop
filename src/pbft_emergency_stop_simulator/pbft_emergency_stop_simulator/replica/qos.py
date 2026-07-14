"""QoS profile factories for the PBFT replica.

Depths are configurable so a replica can be tuned for larger n/f
configurations or higher message rates without touching the callback
logic. Defaults match the original hardcoded values.
"""

from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    QoSProfile,
    ReliabilityPolicy,
)


def create_pbft_qos(depth: int = 20) -> QoSProfile:
    """Create the QoS profile used for PBFT protocol messages.

    Args:
        depth: History depth for the protocol topics
            (pre-prepare/prepare/commit/view-change/new-view).
            Increase this for scenarios with many replicas or bursts
            of protocol messages so late subscribers do not drop
            older-but-still-relevant messages.
    """
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=depth,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.VOLATILE,
    )


def create_status_qos(depth: int = 1) -> QoSProfile:
    """Create QoS for the latest replica status.

    Args:
        depth: History depth for /pbft/status. 1 is normally enough
            because only the latest status matters, but a monitor
            that wants a short local history can request more.
    """
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=depth,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )
