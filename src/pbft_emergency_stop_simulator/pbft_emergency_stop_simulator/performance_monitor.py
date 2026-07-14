"""Measure PBFT protocol latency and write one table row per request."""

from __future__ import annotations

from collections import Counter
import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any
from uuid import uuid4

import rclpy
from rclpy.node import Node
from rclpy.qos import (
    DurabilityPolicy,
    HistoryPolicy,
    QoSProfile,
    ReliabilityPolicy,
)
from std_msgs.msg import Bool, String

from pbft_emergency_stop_interfaces.msg import (
    NewView,
    PBFTDecision,
    PBFTMessage,
    ReplicaStatus,
    ViewChange,
)


NANOSECONDS_PER_MILLISECOND = 1_000_000

TERMINAL_SAFETY_STATES = {
    'CONFIRMED_STOP',
    'CONFIRMED_RELEASE',
    'FAIL_SAFE_STOP',
}

CSV_FIELDS = [
    'run_id',
    'scenario_label',
    'request_id',
    'request_digest_prefix',
    'emergency_stop',
    'replica_count',
    'max_faulty',
    'initial_view',
    'decision_view',
    'sequence_number',
    'result',
    'terminal_safety_state',
    'faulty_nodes',
    'faulty_behaviors',
    'started_at_utc',
    'completed_at_utc',
    'request_to_pre_prepare_ms',
    'pre_prepare_to_first_prepare_ms',
    'request_to_first_prepare_ms',
    'request_to_first_prepared_ms',
    'prepared_to_first_commit_ms',
    'request_to_first_commit_ms',
    'request_to_first_committed_ms',
    'request_to_decision_ms',
    'source_stamp_request_to_decision_ms',
    'request_to_first_view_change_ms',
    'first_view_change_to_new_view_ms',
    'new_view_to_recovery_pre_prepare_ms',
    'new_view_to_decision_ms',
    'first_view_change_to_decision_ms',
    'decision_to_safety_output_ms',
    'request_to_terminal_safety_state_ms',
    'result_latency_ms',
    'request_messages',
    'pre_prepare_messages',
    'prepare_messages',
    'commit_messages',
    'status_messages',
    'view_change_messages',
    'new_view_messages',
    'decision_messages',
    'safety_state_messages',
    'safety_output_messages',
    'total_protocol_messages',
    'total_observed_messages',
    'duplicate_protocol_messages',
    'unique_prepare_senders',
    'unique_commit_senders',
    'unique_view_change_senders',
    'views_seen',
]


def volatile_qos(depth: int = 200) -> QoSProfile:
    """Return reliable volatile QoS for PBFT protocol topics."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=depth,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.VOLATILE,
    )


def transient_qos(depth: int = 50) -> QoSProfile:
    """Return reliable transient-local QoS for state and decision topics."""
    return QoSProfile(
        history=HistoryPolicy.KEEP_LAST,
        depth=depth,
        reliability=ReliabilityPolicy.RELIABLE,
        durability=DurabilityPolicy.TRANSIENT_LOCAL,
    )


def message_stamp_ns(message: Any) -> int:
    """Convert a stamped ROS message timestamp to nanoseconds."""
    stamp = getattr(message, 'stamp', None)
    if stamp is None:
        return 0
    return int(stamp.sec) * 1_000_000_000 + int(stamp.nanosec)


def utc_now_text() -> str:
    """Return the current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds')


@dataclass
class RequestMeasurement:
    """Store timing and message-count data for one client request."""

    run_id: str
    scenario_label: str
    request_id: str
    request_digest: str
    emergency_stop: bool
    replica_count: int
    max_faulty: int
    initial_view: int
    faulty_nodes: str
    faulty_behaviors: str
    started_at_utc: str

    first_arrival_ns: dict[str, int] = field(default_factory=dict)
    first_source_stamp_ns: dict[str, int] = field(default_factory=dict)
    counts: Counter[str] = field(default_factory=Counter)
    unique_senders: dict[str, set[int]] = field(
        default_factory=lambda: {
            'prepare': set(),
            'commit': set(),
            'view_change': set(),
        }
    )
    views_seen: set[int] = field(default_factory=set)
    seen_protocol_fingerprints: set[tuple[Any, ...]] = field(
        default_factory=set
    )

    decision_view: int | None = None
    sequence_number: int | None = None
    decision_committed: bool | None = None
    terminal_safety_state: str = ''
    completed_at_utc: str = ''
    result: str = 'RUNNING'
    completion_arrival_ns: int | None = None
    finalized: bool = False

    def record(
        self,
        event_name: str,
        arrival_ns: int,
        source_stamp_ns: int = 0,
    ) -> None:
        """Record the first timestamp and increment the event counter."""
        self.counts[event_name] += 1
        self.first_arrival_ns.setdefault(event_name, arrival_ns)
        if source_stamp_ns > 0:
            self.first_source_stamp_ns.setdefault(
                event_name,
                source_stamp_ns,
            )


class PBFTPerformanceMonitor(Node):
    """Observe PBFT topics and persist performance metrics to CSV and Markdown."""

    def __init__(self) -> None:
        super().__init__('pbft_performance_monitor')

        self.declare_parameter('replica_count', 4)
        self.declare_parameter('max_faulty', 1)
        self.declare_parameter('scenario_label', 'manual')
        self.declare_parameter('faulty_nodes', 'none')
        self.declare_parameter('faulty_behaviors', 'none')
        self.declare_parameter(
            'output_csv',
            '~/.ros/pbft_performance/performance_results.csv',
        )
        self.declare_parameter(
            'output_markdown',
            '~/.ros/pbft_performance/performance_results.md',
        )
        self.declare_parameter(
            'output_jsonl',
            '~/.ros/pbft_performance/performance_results.jsonl',
        )
        self.declare_parameter('measurement_timeout_sec', 30.0)
        self.declare_parameter('finalize_delay_sec', 0.5)
        self.declare_parameter('request_id_filter', '')
        self.declare_parameter('truncate_output_on_start', False)
        self.declare_parameter('log_each_protocol_message', False)

        self.replica_count = int(
            self.get_parameter('replica_count').value
        )
        self.max_faulty = int(
            self.get_parameter('max_faulty').value
        )
        self.scenario_label = str(
            self.get_parameter('scenario_label').value
        )
        self.faulty_nodes = str(
            self.get_parameter('faulty_nodes').value
        )
        self.faulty_behaviors = str(
            self.get_parameter('faulty_behaviors').value
        )
        self.measurement_timeout_sec = float(
            self.get_parameter('measurement_timeout_sec').value
        )
        self.finalize_delay_sec = float(
            self.get_parameter('finalize_delay_sec').value
        )
        self.request_id_filter = str(
            self.get_parameter('request_id_filter').value
        ).strip()
        self.truncate_output_on_start = bool(
            self.get_parameter('truncate_output_on_start').value
        )
        self.log_each_protocol_message = bool(
            self.get_parameter('log_each_protocol_message').value
        )

        self.output_csv = Path(
            str(self.get_parameter('output_csv').value)
        ).expanduser()
        self.output_markdown = Path(
            str(self.get_parameter('output_markdown').value)
        ).expanduser()
        self.output_jsonl = Path(
            str(self.get_parameter('output_jsonl').value)
        ).expanduser()

        self._validate_parameters()
        self._prepare_output_files()

        self.active_by_request_id: dict[str, RequestMeasurement] = {}
        self.all_measurements: list[RequestMeasurement] = []
        self.current_measurement: RequestMeasurement | None = None

        self._subscriptions = [
            self.create_subscription(
                PBFTMessage,
                '/pbft/request',
                lambda message: self._protocol_callback(
                    'request',
                    message,
                ),
                volatile_qos(),
            ),
            self.create_subscription(
                PBFTMessage,
                '/pbft/pre_prepare',
                lambda message: self._protocol_callback(
                    'pre_prepare',
                    message,
                ),
                volatile_qos(),
            ),
            self.create_subscription(
                PBFTMessage,
                '/pbft/prepare',
                lambda message: self._protocol_callback(
                    'prepare',
                    message,
                ),
                volatile_qos(),
            ),
            self.create_subscription(
                PBFTMessage,
                '/pbft/commit',
                lambda message: self._protocol_callback(
                    'commit',
                    message,
                ),
                volatile_qos(),
            ),
            self.create_subscription(
                ReplicaStatus,
                '/pbft/status',
                self._status_callback,
                transient_qos(100),
            ),
            self.create_subscription(
                ViewChange,
                '/pbft/view_change',
                self._view_change_callback,
                volatile_qos(),
            ),
            self.create_subscription(
                NewView,
                '/pbft/new_view',
                self._new_view_callback,
                volatile_qos(),
            ),
            self.create_subscription(
                PBFTDecision,
                '/pbft/decision',
                self._decision_callback,
                transient_qos(),
            ),
            self.create_subscription(
                String,
                '/safety/state',
                self._safety_state_callback,
                transient_qos(20),
            ),
            self.create_subscription(
                Bool,
                '/vehicle/emergency_stop',
                self._safety_output_callback,
                transient_qos(20),
            ),
        ]

        self.finalization_timer = self.create_timer(
            0.1,
            self._check_finalization,
        )

        self.get_logger().info(
            'PBFT performance monitor started: '
            f'n={self.replica_count}, '
            f'f={self.max_faulty}, '
            f'scenario={self.scenario_label}, '
            f'csv={self.output_csv}'
        )

    def _validate_parameters(self) -> None:
        """Validate monitor configuration."""
        if self.replica_count <= 0:
            raise ValueError('replica_count must be positive.')
        if self.max_faulty < 0:
            raise ValueError('max_faulty must be non-negative.')
        if self.measurement_timeout_sec <= 0.0:
            raise ValueError(
                'measurement_timeout_sec must be positive.'
            )
        if self.finalize_delay_sec < 0.0:
            raise ValueError(
                'finalize_delay_sec must be non-negative.'
            )

        expected = 3 * self.max_faulty + 1
        if self.replica_count != expected:
            self.get_logger().warning(
                'Observed configuration is not the simulator canonical '
                f'n=3f+1 form: n={self.replica_count}, '
                f'f={self.max_faulty}, expected_n={expected}.'
            )

    def _prepare_output_files(self) -> None:
        """Create result directories and optionally clear old files."""
        for path in (
            self.output_csv,
            self.output_markdown,
            self.output_jsonl,
        ):
            path.parent.mkdir(parents=True, exist_ok=True)

        if self.truncate_output_on_start:
            for path in (
                self.output_csv,
                self.output_markdown,
                self.output_jsonl,
            ):
                if path.exists():
                    path.unlink()

    def _protocol_callback(
        self,
        event_name: str,
        message: PBFTMessage,
    ) -> None:
        """Process REQUEST, PRE-PREPARE, PREPARE or COMMIT."""
        now_ns = time.monotonic_ns()
        source_ns = message_stamp_ns(message)

        if event_name == 'request':
            measurement = self._start_or_get_measurement(
                message,
                now_ns,
                source_ns,
            )
            if measurement is None:
                return
        else:
            measurement = self._find_measurement(
                message.request_id,
            )
            if measurement is None:
                return

        if (
            event_name != 'request'
            and self._is_stale_source_message(
                measurement,
                source_ns,
            )
        ):
            return

        measurement.record(
            event_name,
            now_ns,
            source_ns,
        )
        measurement.views_seen.add(int(message.view))

        fingerprint = (
            event_name,
            int(message.sender_id),
            int(message.recipient_id),
            int(message.view),
            int(message.sequence_number),
            message.request_id,
            message.request_digest,
            bool(message.emergency_stop),
        )

        if fingerprint in measurement.seen_protocol_fingerprints:
            measurement.counts['duplicate_protocol'] += 1
        else:
            measurement.seen_protocol_fingerprints.add(fingerprint)

        if event_name in {'prepare', 'commit'}:
            measurement.unique_senders[event_name].add(
                int(message.sender_id)
            )

        if (
            event_name == 'pre_prepare'
            and int(message.view) > measurement.initial_view
        ):
            measurement.record(
                'recovery_pre_prepare',
                now_ns,
                source_ns,
            )

        if self.log_each_protocol_message:
            self.get_logger().info(
                'Observed protocol message: '
                f'type={event_name}, '
                f'sender={message.sender_id}, '
                f'view={message.view}, '
                f'sequence={message.sequence_number}, '
                f'request_id={message.request_id}'
            )

    def _start_or_get_measurement(
        self,
        message: PBFTMessage,
        now_ns: int,
        source_ns: int,
    ) -> RequestMeasurement | None:
        """Start one measurement when a new REQUEST is observed."""
        request_id = message.request_id.strip()
        if not request_id:
            self.get_logger().warning(
                'Ignored REQUEST with an empty request_id.'
            )
            return None

        if (
            self.request_id_filter
            and request_id != self.request_id_filter
        ):
            return None

        existing = self.active_by_request_id.get(request_id)
        if existing is not None:
            return existing

        run_id = (
            datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
            + '-'
            + uuid4().hex[:8]
        )

        measurement = RequestMeasurement(
            run_id=run_id,
            scenario_label=self.scenario_label,
            request_id=request_id,
            request_digest=message.request_digest,
            emergency_stop=bool(message.emergency_stop),
            replica_count=self.replica_count,
            max_faulty=self.max_faulty,
            initial_view=int(message.view),
            faulty_nodes=self.faulty_nodes,
            faulty_behaviors=self.faulty_behaviors,
            started_at_utc=utc_now_text(),
        )
        measurement.views_seen.add(int(message.view))

        self.active_by_request_id[request_id] = measurement
        self.all_measurements.append(measurement)
        self.current_measurement = measurement

        self.get_logger().info(
            'Started PBFT performance measurement: '
            f'run_id={run_id}, '
            f'request_id={request_id}, '
            f'initial_view={message.view}'
        )
        return measurement

    def _find_measurement(
        self,
        request_id: str = '',
    ) -> RequestMeasurement | None:
        """Find an active measurement without mixing different requests."""
        normalized = request_id.strip()

        # A message that carries a request_id must match it exactly.
        # Falling back to the only active request here can associate a
        # transient-local decision from an older run with a new request.
        if normalized:
            return self.active_by_request_id.get(normalized)

        # VIEW-CHANGE/NEW-VIEW and safety messages can legitimately carry no
        # request_id, so an empty identifier may use the sole active request.
        active = [
            item
            for item in self.active_by_request_id.values()
            if not item.finalized
        ]

        if len(active) == 1:
            return active[0]

        if (
            self.current_measurement is not None
            and not self.current_measurement.finalized
        ):
            return self.current_measurement

        return None

    @staticmethod
    def _is_stale_source_message(
        measurement: RequestMeasurement,
        source_stamp_ns: int,
    ) -> bool:
        """Return True when a stamped event predates the active request."""
        request_stamp_ns = measurement.first_source_stamp_ns.get(
            'request',
            0,
        )
        return (
            source_stamp_ns > 0
            and request_stamp_ns > 0
            and source_stamp_ns < request_stamp_ns
        )

    def _status_callback(self, message: ReplicaStatus) -> None:
        """Record prepared and committed local replica states."""
        measurement = self._find_measurement(message.request_id)
        if measurement is None:
            return

        now_ns = time.monotonic_ns()
        source_ns = message_stamp_ns(message)

        if self._is_stale_source_message(
            measurement,
            source_ns,
        ):
            return

        measurement.record('status', now_ns, source_ns)
        measurement.views_seen.add(int(message.view))

        if bool(message.prepared):
            measurement.record(
                'prepared_status',
                now_ns,
                source_ns,
            )

        if bool(message.committed):
            measurement.record(
                'committed_status',
                now_ns,
                source_ns,
            )

    def _view_change_callback(self, message: ViewChange) -> None:
        """Record the first VIEW-CHANGE and count its senders."""
        measurement = self._find_measurement(message.request_id)
        if measurement is None:
            return

        now_ns = time.monotonic_ns()
        source_ns = message_stamp_ns(message)

        if self._is_stale_source_message(
            measurement,
            source_ns,
        ):
            return

        measurement.record('view_change', now_ns, source_ns)
        measurement.unique_senders['view_change'].add(
            int(message.sender_id)
        )
        measurement.views_seen.add(int(message.new_view))

    def _new_view_callback(self, message: NewView) -> None:
        """Record activation of a new PBFT view."""
        measurement = self._find_measurement(message.request_id)
        if measurement is None:
            return

        now_ns = time.monotonic_ns()
        source_ns = message_stamp_ns(message)

        if self._is_stale_source_message(
            measurement,
            source_ns,
        ):
            return

        measurement.record('new_view', now_ns, source_ns)
        measurement.views_seen.add(int(message.new_view))

    def _decision_callback(self, message: PBFTDecision) -> None:
        """Record the externally confirmed PBFT decision."""
        measurement = self._find_measurement(message.request_id)
        if measurement is None:
            return

        source_ns = message_stamp_ns(message)
        if self._is_stale_source_message(
            measurement,
            source_ns,
        ):
            self.get_logger().warning(
                'Ignored stale decision older than the active request: '
                f'request_id={message.request_id}.'
            )
            return

        now_ns = time.monotonic_ns()
        measurement.record('decision', now_ns, source_ns)
        measurement.decision_view = int(message.view)
        measurement.sequence_number = int(message.sequence_number)
        measurement.decision_committed = bool(message.committed)
        measurement.views_seen.add(int(message.view))

        if message.committed:
            measurement.result = 'COMMITTED'
            measurement.completion_arrival_ns = now_ns
        else:
            measurement.result = 'DECISION_NOT_COMMITTED'
            measurement.completion_arrival_ns = now_ns

    def _safety_state_callback(self, message: String) -> None:
        """Associate a safety state with the current request."""
        measurement = self._find_measurement()
        if measurement is None:
            return

        now_ns = time.monotonic_ns()
        measurement.record('safety_state', now_ns)

        if message.data in TERMINAL_SAFETY_STATES:
            if not measurement.terminal_safety_state:
                measurement.terminal_safety_state = message.data
                measurement.record(
                    'terminal_safety_state',
                    now_ns,
                )

            if (
                message.data == 'FAIL_SAFE_STOP'
                and measurement.decision_committed is not True
            ):
                measurement.result = 'FAIL_SAFE_STOP'
                measurement.completion_arrival_ns = now_ns

    def _safety_output_callback(self, message: Bool) -> None:
        """Record safety output publication associated with the request."""
        measurement = self._find_measurement()
        if measurement is None:
            return

        now_ns = time.monotonic_ns()
        measurement.record('safety_output', now_ns)

        if 'decision' in measurement.first_arrival_ns:
            measurement.record(
                'safety_output_after_decision',
                now_ns,
            )

    def _check_finalization(self) -> None:
        """Finalize completed, failed or timed-out measurements."""
        now_ns = time.monotonic_ns()

        for measurement in list(
            self.active_by_request_id.values()
        ):
            request_ns = measurement.first_arrival_ns['request']
            request_age_sec = (
                now_ns - request_ns
            ) / 1_000_000_000

            completion_ns = measurement.completion_arrival_ns
            if completion_ns is not None:
                completion_age_sec = (
                    now_ns - completion_ns
                ) / 1_000_000_000
                if completion_age_sec >= self.finalize_delay_sec:
                    self._finalize_measurement(measurement)
                    continue

            if request_age_sec >= self.measurement_timeout_sec:
                measurement.result = 'MEASUREMENT_TIMEOUT'
                measurement.completion_arrival_ns = now_ns
                self._finalize_measurement(measurement)

    def _finalize_measurement(
        self,
        measurement: RequestMeasurement,
    ) -> None:
        """Write one completed measurement to all output formats."""
        if measurement.finalized:
            return

        measurement.finalized = True
        measurement.completed_at_utc = utc_now_text()

        row = self._build_row(measurement)
        self._append_csv(row)
        self._append_jsonl(row)
        self._regenerate_markdown()

        self.active_by_request_id.pop(
            measurement.request_id,
            None,
        )
        if self.current_measurement is measurement:
            active = list(self.active_by_request_id.values())
            self.current_measurement = (
                active[-1] if active else None
            )

        consensus_ms = row['request_to_decision_ms']
        view_change_ms = row[
            'first_view_change_to_new_view_ms'
        ]

        self.get_logger().info(
            'PBFT PERFORMANCE RESULT: '
            f'run_id={measurement.run_id}, '
            f'scenario={measurement.scenario_label}, '
            f'request_id={measurement.request_id}, '
            f'result={measurement.result}, '
            f'consensus_ms={consensus_ms}, '
            f'view_change_ms={view_change_ms}, '
            f'protocol_messages={row["total_protocol_messages"]}, '
            f'csv={self.output_csv}'
        )

    def finalize_pending(self, result: str = 'INTERRUPTED') -> None:
        """Persist all active measurements before node shutdown."""
        now_ns = time.monotonic_ns()
        for measurement in list(
            self.active_by_request_id.values()
        ):
            if measurement.finalized:
                continue
            measurement.result = result
            measurement.completion_arrival_ns = now_ns
            self._finalize_measurement(measurement)

    def _build_row(
        self,
        measurement: RequestMeasurement,
    ) -> dict[str, Any]:
        """Build the stable CSV row for one request."""
        arrivals = measurement.first_arrival_ns
        sources = measurement.first_source_stamp_ns
        counts = measurement.counts

        completion_ns = (
            measurement.completion_arrival_ns
            or time.monotonic_ns()
        )

        protocol_count = sum(
            counts[name]
            for name in (
                'request',
                'pre_prepare',
                'prepare',
                'commit',
                'view_change',
                'new_view',
                'decision',
            )
        )
        total_count = protocol_count + sum(
            counts[name]
            for name in (
                'status',
                'safety_state',
                'safety_output',
            )
        )

        row = {
            'run_id': measurement.run_id,
            'scenario_label': measurement.scenario_label,
            'request_id': measurement.request_id,
            'request_digest_prefix': measurement.request_digest[:12],
            'emergency_stop': measurement.emergency_stop,
            'replica_count': measurement.replica_count,
            'max_faulty': measurement.max_faulty,
            'initial_view': measurement.initial_view,
            'decision_view': self._optional_value(
                measurement.decision_view
            ),
            'sequence_number': self._optional_value(
                measurement.sequence_number
            ),
            'result': measurement.result,
            'terminal_safety_state': (
                measurement.terminal_safety_state
            ),
            'faulty_nodes': measurement.faulty_nodes,
            'faulty_behaviors': measurement.faulty_behaviors,
            'started_at_utc': measurement.started_at_utc,
            'completed_at_utc': measurement.completed_at_utc,
            'request_to_pre_prepare_ms': self._delta_ms(
                arrivals,
                'request',
                'pre_prepare',
            ),
            'pre_prepare_to_first_prepare_ms': self._delta_ms(
                arrivals,
                'pre_prepare',
                'prepare',
            ),
            'request_to_first_prepare_ms': self._delta_ms(
                arrivals,
                'request',
                'prepare',
            ),
            'request_to_first_prepared_ms': self._delta_ms(
                arrivals,
                'request',
                'prepared_status',
            ),
            'prepared_to_first_commit_ms': self._delta_ms(
                arrivals,
                'prepared_status',
                'commit',
            ),
            'request_to_first_commit_ms': self._delta_ms(
                arrivals,
                'request',
                'commit',
            ),
            'request_to_first_committed_ms': self._delta_ms(
                arrivals,
                'request',
                'committed_status',
            ),
            'request_to_decision_ms': self._delta_ms(
                arrivals,
                'request',
                'decision',
            ),
            'source_stamp_request_to_decision_ms': (
                self._delta_ms(
                    sources,
                    'request',
                    'decision',
                )
            ),
            'request_to_first_view_change_ms': self._delta_ms(
                arrivals,
                'request',
                'view_change',
            ),
            'first_view_change_to_new_view_ms': self._delta_ms(
                arrivals,
                'view_change',
                'new_view',
            ),
            'new_view_to_recovery_pre_prepare_ms': self._delta_ms(
                arrivals,
                'new_view',
                'recovery_pre_prepare',
            ),
            'new_view_to_decision_ms': self._delta_ms(
                arrivals,
                'new_view',
                'decision',
            ),
            'first_view_change_to_decision_ms': self._delta_ms(
                arrivals,
                'view_change',
                'decision',
            ),
            'decision_to_safety_output_ms': self._delta_ms(
                arrivals,
                'decision',
                'safety_output_after_decision',
            ),
            'request_to_terminal_safety_state_ms': self._delta_ms(
                arrivals,
                'request',
                'terminal_safety_state',
            ),
            'result_latency_ms': self._ns_difference_ms(
                arrivals['request'],
                completion_ns,
            ),
            'request_messages': counts['request'],
            'pre_prepare_messages': counts['pre_prepare'],
            'prepare_messages': counts['prepare'],
            'commit_messages': counts['commit'],
            'status_messages': counts['status'],
            'view_change_messages': counts['view_change'],
            'new_view_messages': counts['new_view'],
            'decision_messages': counts['decision'],
            'safety_state_messages': counts['safety_state'],
            'safety_output_messages': counts['safety_output'],
            'total_protocol_messages': protocol_count,
            'total_observed_messages': total_count,
            'duplicate_protocol_messages': counts[
                'duplicate_protocol'
            ],
            'unique_prepare_senders': len(
                measurement.unique_senders['prepare']
            ),
            'unique_commit_senders': len(
                measurement.unique_senders['commit']
            ),
            'unique_view_change_senders': len(
                measurement.unique_senders['view_change']
            ),
            'views_seen': ','.join(
                str(view)
                for view in sorted(measurement.views_seen)
            ),
        }
        return row

    @staticmethod
    def _delta_ms(
        timestamps: dict[str, int],
        start_name: str,
        end_name: str,
    ) -> str | float:
        """Return a rounded millisecond delta or an empty value."""
        start_ns = timestamps.get(start_name)
        end_ns = timestamps.get(end_name)
        if start_ns is None or end_ns is None:
            return ''
        return PBFTPerformanceMonitor._ns_difference_ms(
            start_ns,
            end_ns,
        )

    @staticmethod
    def _ns_difference_ms(
        start_ns: int,
        end_ns: int,
    ) -> float:
        """Convert one nanosecond difference to milliseconds."""
        return round(
            (end_ns - start_ns)
            / NANOSECONDS_PER_MILLISECOND,
            3,
        )

    @staticmethod
    def _optional_value(value: Any) -> Any:
        """Convert None to an empty table cell."""
        return '' if value is None else value

    def _append_csv(self, row: dict[str, Any]) -> None:
        """Append one row to the CSV result table."""
        file_exists = self.output_csv.exists()
        needs_header = (
            not file_exists
            or self.output_csv.stat().st_size == 0
        )

        with self.output_csv.open(
            'a',
            newline='',
            encoding='utf-8',
        ) as stream:
            writer = csv.DictWriter(
                stream,
                fieldnames=CSV_FIELDS,
                extrasaction='ignore',
            )
            if needs_header:
                writer.writeheader()
            writer.writerow(row)

    def _append_jsonl(self, row: dict[str, Any]) -> None:
        """Append the complete result row to a JSON Lines file."""
        with self.output_jsonl.open(
            'a',
            encoding='utf-8',
        ) as stream:
            stream.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                )
                + '\n'
            )

    def _regenerate_markdown(self) -> None:
        """Regenerate a compact human-readable Markdown table."""
        if not self.output_csv.exists():
            return

        with self.output_csv.open(
            'r',
            newline='',
            encoding='utf-8',
        ) as stream:
            rows = list(csv.DictReader(stream))

        headers = [
            'Scenario',
            'Request',
            'n/f',
            'Result',
            'View',
            'Consensus [ms]',
            'View change [ms]',
            'Protocol messages',
            'Safety state',
        ]

        lines = [
            '# PBFT performance results',
            '',
            '| ' + ' | '.join(headers) + ' |',
            '| ' + ' | '.join(['---'] * len(headers)) + ' |',
        ]

        for row in rows:
            values = [
                row.get('scenario_label', ''),
                row.get('request_id', ''),
                (
                    f'{row.get("replica_count", "")}/'
                    f'{row.get("max_faulty", "")}'
                ),
                row.get('result', ''),
                row.get('decision_view', ''),
                row.get('request_to_decision_ms', ''),
                row.get(
                    'first_view_change_to_new_view_ms',
                    '',
                ),
                row.get('total_protocol_messages', ''),
                row.get('terminal_safety_state', ''),
            ]
            escaped = [
                str(value).replace('|', '\\|')
                for value in values
            ]
            lines.append(
                '| ' + ' | '.join(escaped) + ' |'
            )

        lines.extend(
            [
                '',
                (
                    'Detaljni podaci nalaze se u CSV i JSONL '
                    'datotekama.'
                ),
                '',
            ]
        )

        self.output_markdown.write_text(
            '\n'.join(lines),
            encoding='utf-8',
        )


def main(args: list[str] | None = None) -> None:
    """Run the PBFT performance monitor."""
    rclpy.init(args=args)
    node = PBFTPerformanceMonitor()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().info(
            'Stopping PBFT performance monitor.'
        )
    finally:
        node.finalize_pending()
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
