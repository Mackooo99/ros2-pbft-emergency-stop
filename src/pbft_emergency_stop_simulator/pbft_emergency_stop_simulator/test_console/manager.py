"""Sequential scenario orchestration with isolated ROS domains."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime
import json
import os
from pathlib import Path
import signal
import time
from typing import Any
from uuid import uuid4

import yaml

from .catalog import scenario_map
from .configuration import (
    materialize_scenario,
    scenario_compatibility,
    validate_configuration,
)
from .reporting import write_suite_report


class ScenarioManager:
    """Run one PBFT scenario at a time and stream live updates."""

    def __init__(
        self,
        catalog: dict[str, Any],
        results_root: Path,
    ) -> None:
        self.catalog = catalog
        self.scenarios = scenario_map(catalog)
        self.results_root = results_root.expanduser()
        self.results_root.mkdir(parents=True, exist_ok=True)

        self.suites: dict[str, dict[str, Any]] = {}
        self.active_suite_id: str | None = None
        self.active_task: asyncio.Task | None = None
        self.cancel_requested = False
        self.subscribers: set[asyncio.Queue] = set()
        self.domain_counter = 70

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=200)
        self.subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self.subscribers.discard(queue)

    async def broadcast(self, event: dict[str, Any]) -> None:
        dead = []
        for queue in self.subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                    queue.put_nowait(event)
                except Exception:
                    dead.append(queue)
        for queue in dead:
            self.subscribers.discard(queue)

    def list_suites(self) -> list[dict[str, Any]]:
        return sorted(
            (deepcopy(item) for item in self.suites.values()),
            key=lambda item: item.get("created_at", ""),
            reverse=True,
        )

    def get_suite(self, suite_id: str) -> dict[str, Any] | None:
        suite = self.suites.get(suite_id)
        return deepcopy(suite) if suite else None

    async def start_suite(
        self,
        scenario_ids: list[str],
        repeat: int = 1,
        stop_on_failure: bool = False,
        configuration: dict[str, Any] | None = None,
    ) -> str:
        if self.active_task and not self.active_task.done():
            raise RuntimeError("Another PBFT test suite is already running.")

        unknown = [item for item in scenario_ids if item not in self.scenarios]
        if unknown:
            raise ValueError(f"Unknown scenario IDs: {unknown}")
        if repeat < 1 or repeat > 100:
            raise ValueError("repeat must be in range 1..100")

        validation = validate_configuration(configuration)
        if not validation["valid"]:
            raise ValueError("; ".join(validation["errors"]))

        incompatible = []
        for scenario_id in scenario_ids:
            compatibility = scenario_compatibility(
                self.scenarios[scenario_id], validation
            )
            if not compatibility["compatible"]:
                incompatible.append(
                    f"{scenario_id}: {compatibility['reason']}"
                )
        if incompatible:
            raise ValueError(
                "Selected scenarios are incompatible with the active configuration: "
                + "; ".join(incompatible)
            )

        suite_id = datetime.now().strftime("%Y%m%d_%H%M%S_") + uuid4().hex[:6]
        suite_dir = self.results_root / suite_id
        suite_dir.mkdir(parents=True, exist_ok=False)

        queue = []
        for _ in range(repeat):
            queue.extend(scenario_ids)

        suite = {
            "suite_id": suite_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "status": "QUEUED",
            "scenario_ids": scenario_ids,
            "repeat": repeat,
            "stop_on_failure": stop_on_failure,
            "configuration": deepcopy(validation["configuration"]),
            "derived_configuration": deepcopy(validation["derived"]),
            "configuration_warnings": list(validation["warnings"]),
            "queue": queue,
            "active_index": None,
            "active_scenario": None,
            "active_snapshot": None,
            "live_log": [],
            "results": [],
            "report_path": str(suite_dir / "report.html"),
        }

        self.suites[suite_id] = suite
        self.active_suite_id = suite_id
        self.cancel_requested = False
        self.active_task = asyncio.create_task(self._run_suite(suite_id, suite_dir))
        await self.broadcast({"type": "suite_created", "suite": deepcopy(suite)})
        return suite_id

    async def cancel_active(self) -> None:
        self.cancel_requested = True
        await self.broadcast({"type": "cancel_requested"})

    async def _run_suite(self, suite_id: str, suite_dir: Path) -> None:
        suite = self.suites[suite_id]
        suite["status"] = "RUNNING"
        await self._publish_suite(suite)

        try:
            for index, scenario_id in enumerate(suite["queue"]):
                if self.cancel_requested:
                    suite["status"] = "CANCELLED"
                    break

                scenario = deepcopy(self.scenarios[scenario_id])
                suite["active_index"] = index
                suite["active_scenario"] = scenario_id
                suite["active_snapshot"] = None
                suite["live_log"] = []
                await self._publish_suite(suite)

                run_dir = suite_dir / f"{index + 1:03d}_{scenario_id}"
                result = await self._run_scenario(
                    suite=suite,
                    scenario=scenario,
                    run_dir=run_dir,
                )
                suite["results"].append(result)
                write_suite_report(suite, suite_dir)
                await self._publish_suite(suite)

                if suite["stop_on_failure"] and result["status"] != "PASS":
                    suite["status"] = "STOPPED_ON_FAILURE"
                    break
            else:
                suite["status"] = "COMPLETED"

            if self.cancel_requested:
                suite["status"] = "CANCELLED"
        except Exception as exc:
            suite["status"] = "ERROR"
            suite["error"] = str(exc)
        finally:
            suite["active_scenario"] = None
            suite["active_snapshot"] = None
            suite["finished_at"] = datetime.now().isoformat(timespec="seconds")
            write_suite_report(suite, suite_dir)
            await self._publish_suite(suite)
            self.active_suite_id = None

    async def _run_scenario(
        self,
        suite: dict[str, Any],
        scenario: dict[str, Any],
        run_dir: Path,
    ) -> dict[str, Any]:
        run_dir.mkdir(parents=True, exist_ok=False)
        validation = validate_configuration(suite.get("configuration"))
        scenario = materialize_scenario(
            scenario=scenario,
            validation=validation,
            cluster_config_path=run_dir / "cluster_config.yaml",
        )
        self.domain_counter += 1
        if self.domain_counter > 220:
            self.domain_counter = 70
        domain_id = self.domain_counter

        runtime_scenario = run_dir / "scenario.yaml"
        result_file = run_dir / "result.json"
        snapshot_file = run_dir / "snapshot.json"
        event_file = run_dir / "events.jsonl"
        runtime_scenario.write_text(
            yaml.safe_dump(scenario, sort_keys=False), encoding="utf-8"
        )

        environment = os.environ.copy()
        environment["ROS_DOMAIN_ID"] = str(domain_id)
        environment["ROS_LOG_DIR"] = str(run_dir / "ros_logs")
        environment["PYTHONUNBUFFERED"] = "1"

        processes: list[tuple[str, asyncio.subprocess.Process]] = []
        log_tasks: list[asyncio.Task] = []

        async def spawn(name: str, command: list[str]) -> asyncio.subprocess.Process:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=environment,
                start_new_session=True,
            )
            processes.append((name, process))
            log_tasks.append(
                asyncio.create_task(
                    self._stream_output(
                        suite=suite,
                        process_name=name,
                        process=process,
                        log_path=run_dir / f"{name}.log",
                    )
                )
            )
            return process

        evaluator_command = [
            "ros2",
            "run",
            "pbft_emergency_stop_simulator",
            "scenario_evaluator",
            "--ros-args",
            "-p",
            f"scenario_file:={runtime_scenario}",
            "-p",
            f"result_file:={result_file}",
            "-p",
            f"snapshot_file:={snapshot_file}",
            "-p",
            f"event_file:={event_file}",
        ]

        evaluator = await spawn("evaluator", evaluator_command)
        await asyncio.sleep(0.5)

        supervisor_config = scenario.get("supervisor", {})
        if supervisor_config.get("enabled", True):
            supervisor_command = [
                "ros2",
                "run",
                "pbft_emergency_stop_simulator",
                "safety_supervisor",
                "--ros-args",
            ]
            for key, value in supervisor_config.get("parameters", {}).items():
                supervisor_command.extend(["-p", f"{key}:={_ros_parameter(value)}"])
            await spawn("safety_supervisor", supervisor_command)
            await asyncio.sleep(0.4)

        launch = scenario["launch"]
        launch_command = [
            "ros2",
            "launch",
            str(launch["package"]),
            str(launch["file"]),
        ]
        for key, value in launch.get("arguments", {}).items():
            launch_command.append(f"{key}:={value}")
        await spawn("launch", launch_command)

        client = scenario.get("client", {})
        if client.get("enabled", False):
            await asyncio.sleep(float(client.get("start_delay_sec", 0.8)))
            client_command = [
                "ros2",
                "run",
                "pbft_emergency_stop_simulator",
                "client_node",
                "--ros-args",
            ]
            for key, value in client.get("parameters", {}).items():
                client_command.extend(["-p", f"{key}:={_ros_parameter(value)}"])
            await spawn("client", client_command)

        snapshot_task = asyncio.create_task(
            self._watch_snapshot(suite, snapshot_file)
        )

        hard_timeout = float(scenario.get("timeout_sec", 15.0)) + 5.0
        started = time.monotonic()

        try:
            while True:
                if self.cancel_requested:
                    break
                if evaluator.returncode is not None:
                    break
                if time.monotonic() - started > hard_timeout:
                    break
                await asyncio.sleep(0.15)
        finally:
            snapshot_task.cancel()
            await self._cleanup_processes(processes)
            await asyncio.gather(*log_tasks, return_exceptions=True)

        if result_file.is_file():
            result = json.loads(result_file.read_text(encoding="utf-8"))
        else:
            result = {
                "scenario_id": scenario["id"],
                "scenario_name": scenario.get("name", scenario["id"]),
                "status": "CANCELLED" if self.cancel_requested else "ERROR",
                "terminal_reason": "orchestrator_cancelled" if self.cancel_requested else "missing_evaluator_result",
                "duration_sec": round(time.monotonic() - started, 3),
                "assertions": [],
                "final_state": suite.get("active_snapshot"),
                "ros_domain_id": str(domain_id),
            }

        result["run_directory"] = str(run_dir)
        result["ros_domain_id"] = str(domain_id)
        result["configuration"] = deepcopy(suite.get("configuration"))
        result["derived_configuration"] = deepcopy(
            suite.get("derived_configuration")
        )
        result["actual_byzantine_count"] = scenario.get(
            "actual_byzantine_count"
        )
        result["faulty_replica_ids"] = scenario.get(
            "faulty_replica_ids", []
        )
        result["within_fault_bound"] = scenario.get("within_fault_bound")
        return result

    async def _stream_output(
        self,
        suite: dict[str, Any],
        process_name: str,
        process: asyncio.subprocess.Process,
        log_path: Path,
    ) -> None:
        if process.stdout is None:
            return

        with log_path.open("w", encoding="utf-8") as log_stream:
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                text = line.decode(errors="replace").rstrip()
                log_stream.write(text + "\n")
                log_stream.flush()

                item = {
                    "source": process_name,
                    "line": text,
                    "time": datetime.now().strftime("%H:%M:%S"),
                }
                suite["live_log"].append(item)
                suite["live_log"] = suite["live_log"][-300:]
                await self.broadcast({"type": "log", "item": item})

    async def _watch_snapshot(
        self,
        suite: dict[str, Any],
        snapshot_file: Path,
    ) -> None:
        last_text = ""
        while True:
            await asyncio.sleep(0.2)
            if not snapshot_file.is_file():
                continue
            try:
                text = snapshot_file.read_text(encoding="utf-8")
                if text == last_text:
                    continue
                last_text = text
                suite["active_snapshot"] = json.loads(text)
                await self.broadcast(
                    {
                        "type": "snapshot",
                        "snapshot": deepcopy(suite["active_snapshot"]),
                    }
                )
            except (OSError, json.JSONDecodeError):
                continue

    async def _cleanup_processes(
        self,
        processes: list[tuple[str, asyncio.subprocess.Process]],
    ) -> None:
        for _, process in reversed(processes):
            if process.returncode is None:
                _signal_group(process.pid, signal.SIGINT)

        await asyncio.sleep(1.0)

        for _, process in reversed(processes):
            if process.returncode is None:
                _signal_group(process.pid, signal.SIGTERM)

        await asyncio.sleep(0.8)

        for _, process in reversed(processes):
            if process.returncode is None:
                _signal_group(process.pid, signal.SIGKILL)

        await asyncio.gather(
            *(process.wait() for _, process in processes),
            return_exceptions=True,
        )

    async def _publish_suite(self, suite: dict[str, Any]) -> None:
        await self.broadcast({"type": "suite", "suite": deepcopy(suite)})


def _signal_group(pid: int, selected_signal: signal.Signals) -> None:
    try:
        os.killpg(pid, selected_signal)
    except ProcessLookupError:
        pass


def _ros_parameter(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
