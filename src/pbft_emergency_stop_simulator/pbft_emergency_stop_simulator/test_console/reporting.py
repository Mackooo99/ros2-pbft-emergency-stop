"""HTML and JSON report generation."""

from __future__ import annotations

from datetime import datetime, timezone
import html
import json
from pathlib import Path
from typing import Any


def write_suite_report(suite: dict[str, Any], output_dir: Path) -> None:
    """Write machine-readable and human-readable suite reports."""
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "suite.json").write_text(
        json.dumps(suite, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (output_dir / "report.html").write_text(
        _render_html(suite), encoding="utf-8"
    )


def _render_html(suite: dict[str, Any]) -> str:
    results = suite.get("results", [])
    passed = sum(1 for item in results if item.get("status") == "PASS")
    failed = sum(1 for item in results if item.get("status") == "FAIL")
    timed_out = sum(1 for item in results if item.get("status") == "TIMEOUT")

    rows = []
    details = []

    for result in results:
        status = result.get("status", "UNKNOWN")
        scenario_name = result.get("scenario_name", result.get("scenario_id", ""))
        rows.append(
            "<tr>"
            f"<td><span class='badge {status.lower()}'>{html.escape(status)}</span></td>"
            f"<td>{html.escape(str(scenario_name))}</td>"
            f"<td>{float(result.get('duration_sec', 0.0)):.3f} s</td>"
            f"<td>{html.escape(str(result.get('ros_domain_id', '')))}</td>"
            "</tr>"
        )

        assertion_rows = []
        for assertion in result.get("assertions", []):
            assertion_rows.append(
                "<tr>"
                f"<td>{'✓' if assertion.get('passed') else '✕'}</td>"
                f"<td>{html.escape(str(assertion.get('label', '')))}</td>"
                f"<td><code>{html.escape(str(assertion.get('actual')))}</code></td>"
                f"<td><code>{html.escape(str(assertion.get('expected')))}</code></td>"
                "</tr>"
            )

        details.append(
            f"<details><summary>{html.escape(status)} — {html.escape(str(scenario_name))}</summary>"
            "<table><thead><tr><th></th><th>Assertion</th><th>Actual</th><th>Expected</th></tr></thead>"
            f"<tbody>{''.join(assertion_rows)}</tbody></table></details>"
        )

    generated = datetime.now(timezone.utc).isoformat()
    configuration = suite.get("configuration", {})
    system = configuration.get("system", {})
    derived = suite.get("derived_configuration", {})
    config_summary = (
        f"n={system.get('replica_count', '—')}, "
        f"f={system.get('max_faulty', '—')}, "
        f"initial view={system.get('initial_view', '—')}, "
        f"primary={derived.get('primary_id', '—')}, "
        f"commit quorum={derived.get('commit_threshold', '—')}"
    )

    return f"""<!doctype html>
<html lang='en'>
<head>
<meta charset='utf-8'>
<title>PBFT Test Report</title>
<style>
body{{font-family:Inter,system-ui,sans-serif;background:#0b1020;color:#e7ecff;margin:0;padding:32px}}
main{{max-width:1100px;margin:auto}}
.panel{{background:#141b31;border:1px solid #263150;border-radius:16px;padding:22px;margin-bottom:18px}}
h1,h2{{margin-top:0}} table{{width:100%;border-collapse:collapse}} th,td{{padding:11px;border-bottom:1px solid #263150;text-align:left}}
.badge{{padding:4px 10px;border-radius:999px;font-weight:700}} .pass{{background:#123e31;color:#65f2bc}} .fail{{background:#4b1e2a;color:#ff91aa}} .timeout{{background:#4a3815;color:#ffd77a}}
.metrics{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}} .metric{{background:#0e1528;border-radius:12px;padding:15px}} .metric b{{display:block;font-size:28px}}
details{{background:#141b31;border:1px solid #263150;border-radius:12px;padding:14px;margin:12px 0}} summary{{cursor:pointer;font-weight:700}} code{{color:#a9c3ff}}
</style>
</head>
<body><main>
<div class='panel'><h1>PBFT Test Report</h1><p>Suite: {html.escape(str(suite.get('suite_id','')))}</p><p>Configuration: {html.escape(config_summary)}</p><p>Generated: {html.escape(generated)}</p></div>
<div class='metrics'><div class='metric'>Executed<b>{len(results)}</b></div><div class='metric'>Passed<b>{passed}</b></div><div class='metric'>Failed<b>{failed}</b></div><div class='metric'>Timeout<b>{timed_out}</b></div></div>
<div class='panel'><h2>Results</h2><table><thead><tr><th>Status</th><th>Scenario</th><th>Duration</th><th>ROS domain</th></tr></thead><tbody>{''.join(rows)}</tbody></table></div>
<div class='panel'><h2>Assertions</h2>{''.join(details)}</div>
</main></body></html>"""
