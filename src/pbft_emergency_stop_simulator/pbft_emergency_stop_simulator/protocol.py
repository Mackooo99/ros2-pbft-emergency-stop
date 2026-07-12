"""Shared PBFT protocol helper functions."""

import hashlib


def compute_request_digest(request_id: str, emergency_stop: bool) -> str:
    """Calculate a deterministic SHA-256 digest for an emergency-stop request."""
    canonical_request = (
        f"request_id={request_id};"
        f"emergency_stop={int(emergency_stop)}"
    )

    return hashlib.sha256(
        canonical_request.encode("utf-8")
    ).hexdigest()
