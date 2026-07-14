#!/usr/bin/env bash

APP_DIR="$HOME/pbft_ws/install/pbft_emergency_stop_simulator/lib/pbft_emergency_stop_simulator"

PATTERN="$APP_DIR/(pbft_replica|pbft_monitor|safety_supervisor|client_node|scenario_evaluator)( |$)"

echo "Zaustavljanje PBFT scenario procesa..."

pkill -TERM -f "$PATTERN" 2>/dev/null || true
sleep 2

pkill -KILL -f "$PATTERN" 2>/dev/null || true
sleep 1

echo
echo "Preostali scenario procesi:"
pgrep -af "$PATTERN" || echo "Nema aktivnih PBFT scenario procesa."
