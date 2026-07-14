"""Shared type aliases and data structures for the PBFT replica."""

from dataclasses import dataclass


MessageKey = tuple[int, int]
"""(view, sequence_number) pair identifying one PBFT protocol instance."""


@dataclass(frozen=True)
class PBFTInstance:
    """Locally stored PBFT request data."""

    request_id: str
    request_digest: str
    emergency_stop: bool
