"""ROS 2 observer that evaluates one PBFT scenario from topic data."""

from __future__ import annotations

import json
import os
from pathlib import Path
import time
from typing import Any

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    QoSProfile,
    ReliabilityPolicy,
)
from std_msgs.msg import Bool, String
import yaml

from pbft_emergency_stop_interfaces.msg import (
    NewView,
    PBFTDecision,
    PBFTMessage,
    ReplicaStatus,
    ViewChange,
)

from .protocol import compute_request_digest
from .test_console.conditions import evaluate_condition, evaluate_group


def volatile_qos(depth: int = 100) -> QoSProfile:
    """QoS for volatile PBFT protocol topics."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=depth,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.VOLATILE,
    )


def transient_qos(depth: int = 30) -> QoSProfile:
    """QoS for latched status, decision and safety topics."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=depth,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )


def _stamp_ns(message: Any) -> int:
    """Convert the message stamp to integer nanoseconds."""
    return int(message.stamp.sec) * 1_000_000_000 + int(message.stamp.nanosec)


class ScenarioEvaluator(Node):
    """Collect protocol facts and produce one deterministic PASS/FAIL result."""

    def __init__(self) -> None:
        super().__init__("scenario_evaluator")

        self.declare_parameter("scenario_file", "")
        self.declare_parameter("result_file", "")
        self.declare_parameter("snapshot_file", "")
        self.declare_parameter("event_file", "")

        self.scenario_path = Path(str(self.get_parameter("scenario_file").value))
        self.result_path = Path(str(self.get_parameter("result_file").value))
        self.snapshot_path = Path(str(self.get_parameter("snapshot_file").value))
        self.event_path = Path(str(self.get_parameter("event_file").value))

        if not self.scenario_path.is_file():
            raise ValueError(f"Scenario file not found: {self.scenario_path}")

        with self.scenario_path.open("r", encoding="utf-8") as stream:
            self.scenario: dict[str, Any] = yaml.safe_load(stream)

        self.replica_count = int(self.scenario.get("replica_count", 4))
        self.timeout_sec = float(self.scenario.get("timeout_sec", 15.0))
        self.start_monotonic = time.monotonic()
        self.finished = False

        self._seen_exact_messages: set[tuple[Any, ...]] = set()
        self._value_fingerprints: dict[str, dict[str, set[tuple[Any, ...]]]] = {
            "request": {},
            "pre_prepare": {},
            "prepare": {},
            "commit": {},
        }
        self._ever_prepared_by_view: dict[str, set[int]] = {}
        self._ever_committed_by_view: dict[str, set[int]] = {}

        self.state: dict[str, Any] = {
            "scenario_id": self.scenario["id"],
            "status": "RUNNING",
            "elapsed_sec": 0.0,
            "current_view": 0,
            "primary_id": 0,
            "request_reference": None,
            "replicas": {},
            "replica_summary": {
                "prepared_count": 0,
                "committed_count": 0,
                "byzantine_count": 0,
                "by_view": {},
                "phase_counts": {},
            },
            "history": {
                "prepared_replicas_by_view": {},
                "committed_replicas_by_view": {},
            },
            "replica_detail_history": {},
            "decision": None,
            "safety": {
                "state": None,
                "emergency_stop": None,
            },
            "counts": {
                "request": 0,
                "pre_prepare": 0,
                "prepare": 0,
                "commit": 0,
                "view_change": 0,
                "new_view": 0,
                "recovery_pre_prepare": 0,
                "pre_prepare_by_view": {},
                "prepare_by_view": {},
                "commit_by_view": {},
                "view_change_by_view": {},
                "view_change_unique_senders_by_view": {},
                "view_change_with_prepared_certificate": 0,
                "new_view_by_view": {},
                "request_by_sender": {},
                "pre_prepare_by_sender": {},
                "prepare_by_sender": {},
                "commit_by_sender": {},
                "duplicate_protocol_messages": 0,
                "duplicate_by_type": {},
                "duplicate_by_sender": {},
                "unique_payload_count": {
                    "request": {},
                    "pre_prepare": {},
                    "prepare": {},
                    "commit": {},
                },
                "invalid_digest": 0,
                "invalid_digest_by_sender": {},
                "zero_sequence": 0,
                "zero_sequence_by_sender": {},
                "unexpected_view": 0,
                "unexpected_view_by_sender": {},
                "value_mismatch": 0,
                "value_mismatch_by_sender": {},
                "invalid_sender": 0,
                "invalid_sender_values": [],
            },
            "first_seen_sec": {
                "request_by_sender": {},
                "pre_prepare_by_sender": {},
                "prepare_by_sender": {},
                "commit_by_sender": {},
            },
            "first_stamp_ns": {
                "request_by_sender": {},
                "pre_prepare_by_sender": {},
                "prepare_by_sender": {},
                "commit_by_sender": {},
            },
            "last": {},
            "timeline": [],
            "assertions": [],
        }

        self._subscriptions = [
            self.create_subscription(
                PBFTMessage,
                "/pbft/request",
                lambda message: self._protocol_callback("request", message),
                volatile_qos(),
            ),
            self.create_subscription(
                PBFTMessage,
                "/pbft/pre_prepare",
                lambda message: self._protocol_callback("pre_prepare", message),
                volatile_qos(),
            ),
            self.create_subscription(
                PBFTMessage,
                "/pbft/prepare",
                lambda message: self._protocol_callback("prepare", message),
                volatile_qos(),
            ),
            self.create_subscription(
                PBFTMessage,
                "/pbft/commit",
                lambda message: self._protocol_callback("commit", message),
                volatile_qos(),
            ),
            self.create_subscription(
                ReplicaStatus,
                "/pbft/status",
                self._status_callback,
                transient_qos(20),
            ),
            self.create_subscription(
                ViewChange,
                "/pbft/view_change",
                self._view_change_callback,
                volatile_qos(),
            ),
            self.create_subscription(
                NewView,
                "/pbft/new_view",
                self._new_view_callback,
                volatile_qos(),
            ),
            self.create_subscription(
                PBFTDecision,
                "/pbft/decision",
                self._decision_callback,
                transient_qos(),
            ),
            self.create_subscription(
                String,
                "/safety/state",
                self._safety_state_callback,
                transient_qos(10),
            ),
            self.create_subscription(
                Bool,
                "/vehicle/emergency_stop",
                self._safety_output_callback,
                transient_qos(10),
            ),
        ]

        self.evaluation_timer = self.create_timer(0.1, self._evaluate)
        self.snapshot_timer = self.create_timer(0.25, self._write_snapshot)

        self._record_event(
            "evaluator_started",
            {
                "scenario_id": self.scenario["id"],
                "timeout_sec": self.timeout_sec,
                "ros_domain_id": os.environ.get("ROS_DOMAIN_ID"),
            },
        )

    def _protocol_callback(self, event_type: str, message: PBFTMessage) -> None:
        elapsed = round(time.monotonic() - self.start_monotonic, 6)
        sender_key = str(int(message.sender_id))
        view_key = str(int(message.view))
        stamp_ns = _stamp_ns(message)

        payload = {
            "message_type": int(message.message_type),
            "sender_id": int(message.sender_id),
            "recipient_id": int(message.recipient_id),
            "view": int(message.view),
            "sequence_number": int(message.sequence_number),
            "request_id": message.request_id,
            "request_digest": message.request_digest,
            "emergency_stop": bool(message.emergency_stop),
            "stamp_ns": stamp_ns,
        }

        self.state["counts"][event_type] += 1
        self.state["last"][event_type] = payload
        self._increment_mapping(f"{event_type}_by_sender", sender_key)

        if event_type in {"prepare", "commit"}:
            self._increment_mapping(f"{event_type}_by_view", view_key)

        if event_type == "pre_prepare":
            self._increment_mapping("pre_prepare_by_view", view_key)
            if message.view > 0:
                self.state["counts"]["recovery_pre_prepare"] += 1

        first_seen = self.state["first_seen_sec"][f"{event_type}_by_sender"]
        first_seen.setdefault(sender_key, elapsed)
        first_stamp = self.state["first_stamp_ns"][f"{event_type}_by_sender"]
        first_stamp.setdefault(sender_key, stamp_ns)

        exact_fingerprint = (
            event_type,
            int(message.sender_id),
            int(message.recipient_id),
            int(message.view),
            int(message.sequence_number),
            message.request_id,
            message.request_digest,
            bool(message.emergency_stop),
        )
        if exact_fingerprint in self._seen_exact_messages:
            self.state["counts"]["duplicate_protocol_messages"] += 1
            self._increment_mapping("duplicate_by_type", event_type)
            self._increment_mapping("duplicate_by_sender", sender_key)
        else:
            self._seen_exact_messages.add(exact_fingerprint)

        value_fingerprint = (
            int(message.view),
            int(message.sequence_number),
            message.request_id,
            message.request_digest,
            bool(message.emergency_stop),
        )
        sender_values = self._value_fingerprints[event_type].setdefault(
            sender_key, set()
        )
        sender_values.add(value_fingerprint)
        self.state["counts"]["unique_payload_count"][event_type][sender_key] = (
            len(sender_values)
        )

        if event_type == "request" and self.state["request_reference"] is None:
            self.state["request_reference"] = {
                "request_id": message.request_id,
                "request_digest": message.request_digest,
                "emergency_stop": bool(message.emergency_stop),
                "initial_view": int(message.view),
            }

        self._record_protocol_anomalies(event_type, message)
        self._record_event(event_type, payload)

    def _record_protocol_anomalies(
        self,
        event_type: str,
        message: PBFTMessage,
    ) -> None:
        sender_key = str(int(message.sender_id))

        valid_sender = (
            message.sender_id == -1
            if event_type == "request"
            else 0 <= message.sender_id < self.replica_count
        )
        if not valid_sender:
            self.state["counts"]["invalid_sender"] += 1
            values = self.state["counts"]["invalid_sender_values"]
            if int(message.sender_id) not in values:
                values.append(int(message.sender_id))
                values.sort()

        if event_type != "request" and message.sequence_number == 0:
            self.state["counts"]["zero_sequence"] += 1
            self._increment_mapping("zero_sequence_by_sender", sender_key)

        if message.request_id:
            expected_digest = compute_request_digest(
                message.request_id,
                bool(message.emergency_stop),
            )
            if message.request_digest != expected_digest:
                self.state["counts"]["invalid_digest"] += 1
                self._increment_mapping("invalid_digest_by_sender", sender_key)

        reference = self.state.get("request_reference")
        if reference is None or event_type == "request":
            return

        if int(message.view) != int(reference["initial_view"]):
            self.state["counts"]["unexpected_view"] += 1
            self._increment_mapping("unexpected_view_by_sender", sender_key)

        if bool(message.emergency_stop) != bool(reference["emergency_stop"]):
            self.state["counts"]["value_mismatch"] += 1
            self._increment_mapping("value_mismatch_by_sender", sender_key)

    def _status_callback(self, message: ReplicaStatus) -> None:
        node_id = int(message.node_id)
        node_key = str(node_id)
        status = {
            "node_id": node_id,
            "view": int(message.view),
            "sequence_number": int(message.sequence_number),
            "request_id": message.request_id,
            "request_digest": message.request_digest,
            "phase": message.phase,
            "prepare_count": int(message.prepare_count),
            "commit_count": int(message.commit_count),
            "prepared": bool(message.prepared),
            "committed": bool(message.committed),
            "emergency_stop": bool(message.emergency_stop),
            "is_byzantine": bool(message.is_byzantine),
            "detail": message.detail,
        }

        old = self.state["replicas"].get(node_key)
        self.state["replicas"][node_key] = status

        if message.detail:
            history = self.state["replica_detail_history"].setdefault(node_key, [])
            if not history or history[-1] != message.detail:
                history.append(message.detail)
                del history[:-50]

        view_key = str(int(message.view))
        if message.prepared:
            self._ever_prepared_by_view.setdefault(view_key, set()).add(node_id)
        if message.committed:
            self._ever_committed_by_view.setdefault(view_key, set()).add(node_id)

        self.state["history"]["prepared_replicas_by_view"] = {
            key: sorted(value)
            for key, value in self._ever_prepared_by_view.items()
        }
        self.state["history"]["committed_replicas_by_view"] = {
            key: sorted(value)
            for key, value in self._ever_committed_by_view.items()
        }

        views = [item["view"] for item in self.state["replicas"].values()]
        if views:
            self.state["current_view"] = max(views)
            self.state["primary_id"] = (
                self.state["current_view"] % self.replica_count
            )

        self._recompute_replica_summary()

        if old != status:
            self._record_event("replica_status", status)

    def _recompute_replica_summary(self) -> None:
        statuses = list(self.state["replicas"].values())
        phase_counts: dict[str, int] = {}
        by_view: dict[str, dict[str, int]] = {}

        for item in statuses:
            phase = str(item.get("phase", ""))
            phase_counts[phase] = phase_counts.get(phase, 0) + 1

            view_key = str(int(item.get("view", 0)))
            summary = by_view.setdefault(
                view_key,
                {
                    "replica_count": 0,
                    "prepared_count": 0,
                    "committed_count": 0,
                    "byzantine_count": 0,
                },
            )
            summary["replica_count"] += 1
            summary["prepared_count"] += int(bool(item.get("prepared")))
            summary["committed_count"] += int(bool(item.get("committed")))
            summary["byzantine_count"] += int(bool(item.get("is_byzantine")))

        self.state["replica_summary"] = {
            "prepared_count": sum(int(bool(item.get("prepared"))) for item in statuses),
            "committed_count": sum(int(bool(item.get("committed"))) for item in statuses),
            "byzantine_count": sum(
                int(bool(item.get("is_byzantine"))) for item in statuses
            ),
            "by_view": by_view,
            "phase_counts": phase_counts,
        }

    def _view_change_callback(self, message: ViewChange) -> None:
        payload = {
            "sender_id": int(message.sender_id),
            "new_view": int(message.new_view),
            "has_prepared_certificate": bool(message.has_prepared_certificate),
            "prepared_view": int(message.prepared_view),
            "prepared_sequence_number": int(message.prepared_sequence_number),
            "request_id": message.request_id,
            "request_digest": message.request_digest,
            "emergency_stop": bool(message.emergency_stop),
            "prepare_senders": [int(item) for item in message.prepare_senders],
            "stamp_ns": _stamp_ns(message),
        }

        self.state["counts"]["view_change"] += 1
        self.state["last"]["view_change"] = payload
        view_key = str(message.new_view)
        self._increment_mapping("view_change_by_view", view_key)

        if message.has_prepared_certificate:
            self.state["counts"]["view_change_with_prepared_certificate"] += 1

        senders = self.state["counts"][
            "view_change_unique_senders_by_view"
        ].setdefault(view_key, [])
        if int(message.sender_id) not in senders:
            senders.append(int(message.sender_id))
            senders.sort()

        self._record_event("view_change", payload)

    def _new_view_callback(self, message: NewView) -> None:
        payload = {
            "sender_id": int(message.sender_id),
            "new_view": int(message.new_view),
            "proof_senders": sorted(
                int(item.sender_id) for item in message.view_change_messages
            ),
            "has_selected_request": bool(message.has_selected_request),
            "selected_from_prepared_certificate": bool(
                message.selected_from_prepared_certificate
            ),
            "selected_prepared_view": int(message.selected_prepared_view),
            "selected_sequence_number": int(message.selected_sequence_number),
            "request_id": message.request_id,
            "request_digest": message.request_digest,
            "emergency_stop": bool(message.emergency_stop),
            "stamp_ns": _stamp_ns(message),
        }

        self.state["counts"]["new_view"] += 1
        self.state["last"]["new_view"] = payload
        self._increment_mapping("new_view_by_view", str(message.new_view))
        self._record_event("new_view", payload)

    def _decision_callback(self, message: PBFTDecision) -> None:
        payload = {
            "view": int(message.view),
            "sequence_number": int(message.sequence_number),
            "request_id": message.request_id,
            "request_digest": message.request_digest,
            "emergency_stop": bool(message.emergency_stop),
            "committed": bool(message.committed),
            "confirmation_count": int(message.confirmation_count),
            "required_confirmations": int(message.required_confirmations),
            "confirming_replicas": sorted(
                int(item) for item in message.confirming_replicas
            ),
            "stamp_ns": _stamp_ns(message),
        }
        self.state["decision"] = payload
        self._record_event("decision", payload)

    def _safety_state_callback(self, message: String) -> None:
        if self.state["safety"]["state"] == message.data:
            return
        self.state["safety"]["state"] = message.data
        self._record_event("safety_state", {"state": message.data})

    def _safety_output_callback(self, message: Bool) -> None:
        value = bool(message.data)
        if self.state["safety"]["emergency_stop"] == value:
            return
        self.state["safety"]["emergency_stop"] = value
        self._record_event("safety_output", {"emergency_stop": value})

    def _increment_mapping(self, field: str, key: str) -> None:
        mapping = self.state["counts"][field]
        mapping[key] = int(mapping.get(key, 0)) + 1

    def _record_event(self, event_type: str, payload: dict[str, Any]) -> None:
        elapsed = round(time.monotonic() - self.start_monotonic, 3)
        event = {
            "elapsed_sec": elapsed,
            "type": event_type,
            "payload": payload,
        }
        self.state["timeline"].append(event)
        self.state["timeline"] = self.state["timeline"][-500:]

        self.event_path.parent.mkdir(parents=True, exist_ok=True)
        with self.event_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, ensure_ascii=False) + "\n")

    def _evaluate(self) -> None:
        if self.finished:
            return

        self.state["elapsed_sec"] = round(
            time.monotonic() - self.start_monotonic, 3
        )
        self.state["assertions"] = self._evaluate_assertions()

        complete_group = self.scenario.get("complete_when", {})
        if complete_group and evaluate_group(complete_group, self.state):
            self._finish("COMPLETED")
            return

        if self.state["elapsed_sec"] >= self.timeout_sec:
            self._finish("TIMEOUT")

    def _evaluate_assertions(self) -> list[dict[str, Any]]:
        assertion_results = []
        for assertion in self.scenario.get("assertions", []):
            result = evaluate_condition(assertion, self.state)
            assertion_results.append(
                {
                    "id": assertion.get("id", assertion.get("path", "assertion")),
                    "label": assertion.get("label", assertion.get("id", "Assertion")),
                    "path": assertion.get("path", ""),
                    "operator": result.operator,
                    "expected": result.expected,
                    "actual": result.actual,
                    "passed": result.passed,
                    "required": bool(assertion.get("required", True)),
                }
            )
        return assertion_results

    def _finish(self, terminal_reason: str) -> None:
        assertion_results = self._evaluate_assertions()
        passed = terminal_reason == "COMPLETED"

        for assertion_result in assertion_results:
            if assertion_result["required"] and not assertion_result["passed"]:
                passed = False

        self.state["assertions"] = assertion_results
        self.state["status"] = "PASS" if passed else (
            "TIMEOUT" if terminal_reason == "TIMEOUT" else "FAIL"
        )
        self.state["elapsed_sec"] = round(
            time.monotonic() - self.start_monotonic, 3
        )

        result_document = {
            "scenario_id": self.scenario["id"],
            "scenario_name": self.scenario.get("name", self.scenario["id"]),
            "status": self.state["status"],
            "terminal_reason": terminal_reason,
            "duration_sec": self.state["elapsed_sec"],
            "assertions": assertion_results,
            "final_state": self.state,
            "ros_domain_id": os.environ.get("ROS_DOMAIN_ID"),
        }

        self.result_path.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_json_write(self.result_path, result_document)
        self._write_snapshot()
        self.finished = True

        self.get_logger().info(
            f"SCENARIO RESULT: {self.state['status']} "
            f"scenario_id={self.scenario['id']}"
        )
        rclpy.shutdown()

    def _write_snapshot(self) -> None:
        if not self.snapshot_path:
            return
        self.state["elapsed_sec"] = round(
            time.monotonic() - self.start_monotonic, 3
        )
        self._atomic_json_write(self.snapshot_path, self.state)

    @staticmethod
    def _atomic_json_write(path: Path, data: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        temporary.replace(path)


def main(args=None) -> None:
    """Run the scenario evaluator node."""
    rclpy.init(args=args)
    node = ScenarioEvaluator()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
