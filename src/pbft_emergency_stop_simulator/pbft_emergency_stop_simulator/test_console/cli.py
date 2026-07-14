"""CLI front-end for the same PBFT scenario manager used by the web UI."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .catalog import load_catalog
from .manager import ScenarioManager


async def _run(args) -> int:
    catalog = load_catalog(args.catalog)
    available = [item["id"] for item in catalog["scenarios"]]

    if args.all:
        selected = available
    elif args.scenario:
        selected = args.scenario
    else:
        raise ValueError("Use --all or at least one --scenario.")

    manager = ScenarioManager(
        catalog,
        Path(args.results_dir).expanduser(),
    )
    suite_id = await manager.start_suite(
        selected,
        repeat=args.repeat,
        stop_on_failure=args.stop_on_failure,
    )

    while True:
        suite = manager.get_suite(suite_id)
        if suite is None:
            return 2
        print(
            f"\rSuite {suite_id}: {suite['status']} "
            f"active={suite.get('active_scenario')} "
            f"completed={len(suite.get('results', []))}/{len(suite.get('queue', []))}",
            end="",
            flush=True,
        )
        if suite["status"] not in {"QUEUED", "RUNNING"}:
            print()
            for result in suite.get("results", []):
                print(
                    f"{result['status']:8} "
                    f"{result['scenario_name']} "
                    f"{result.get('duration_sec', 0.0):.3f}s"
                )
            print(f"Report: {suite['report_path']}")
            return 0 if all(
                item["status"] == "PASS" for item in suite.get("results", [])
            ) else 1
        await asyncio.sleep(0.5)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run PBFT scenarios")
    parser.add_argument("--scenario", action="append")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--stop-on-failure", action="store_true")
    parser.add_argument("--catalog", default=None)
    parser.add_argument(
        "--results-dir",
        default=str(Path.home() / ".ros" / "pbft_test_console" / "runs"),
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
