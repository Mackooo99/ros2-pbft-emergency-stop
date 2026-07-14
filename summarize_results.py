#!/usr/bin/env python3

import csv
import math
from pathlib import Path
import statistics
import sys


def read_values(rows, column):
    values = []

    for row in rows:
        raw = row.get(column, '').strip()
        if raw:
            values.append(float(raw))

    return values


def percentile(values, probability):
    ordered = sorted(values)
    index = max(
        0,
        math.ceil(probability * len(ordered)) - 1,
    )
    return ordered[index]


def report(path):
    if not path.exists():
        print(f'Nedostaje: {path}')
        return

    with path.open(newline='', encoding='utf-8') as stream:
        rows = list(csv.DictReader(stream))

    print()
    print('=' * 72)
    print(path.name)
    print('=' * 72)
    print(f'Ukupno redova: {len(rows)}')

    result_counts = {}
    for row in rows:
        result = row.get('result', 'UNKNOWN')
        result_counts[result] = result_counts.get(result, 0) + 1

    print(f'Rezultati: {result_counts}')

    metrics = [
        'request_to_pre_prepare_ms',
        'request_to_first_prepare_ms',
        'request_to_first_commit_ms',
        'request_to_first_committed_ms',
        'request_to_decision_ms',
        'request_to_first_view_change_ms',
        'first_view_change_to_new_view_ms',
        'new_view_to_recovery_pre_prepare_ms',
        'new_view_to_decision_ms',
        'first_view_change_to_decision_ms',
        'request_to_terminal_safety_state_ms',
        'result_latency_ms',
        'total_protocol_messages',
    ]

    for metric in metrics:
        data = read_values(rows, metric)
        if not data:
            continue

        print()
        print(metric)
        print(f'  samples: {len(data)}')
        print(f'  min:     {min(data):.3f}')
        print(f'  mean:    {statistics.fmean(data):.3f}')
        print(f'  p50:     {statistics.median(data):.3f}')
        print(f'  p95:     {percentile(data, 0.95):.3f}')
        print(f'  p99:     {percentile(data, 0.99):.3f}')
        print(f'  max:     {max(data):.3f}')

        if len(data) > 1:
            print(
                f'  stdev:   '
                f'{statistics.stdev(data):.3f}'
            )


for filename in sys.argv[1:]:
    report(Path(filename))
