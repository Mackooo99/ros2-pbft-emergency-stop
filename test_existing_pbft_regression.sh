#!/usr/bin/env bash
set -o pipefail

WORKSPACE="${HOME}/pbft_ws"
PACKAGE="pbft_emergency_stop_simulator"

cd "${WORKSPACE}" || exit 1
set +u
source install/setup.bash
set -u

run_case() {
    local scenario="$1"
    local request_id="$2"
    local expected_log="$3"
    local expect_decision="$4"

    local name="${scenario%.launch.py}"
    local launch_log="${HOME}/${name}_regression.log"
    local decision_log="${HOME}/${name}_regression_decision.log"

    echo
    echo "============================================================"
    echo "Regression: ${scenario}"
    echo "============================================================"

    rm -f "${launch_log}" "${decision_log}"

    setsid ros2 launch "${PACKAGE}" "${scenario}" \
        >"${launch_log}" 2>&1 &
    local launch_pid=$!

    sleep 3

    timeout 10 ros2 topic echo /pbft/decision \
        --qos-reliability reliable \
        --qos-durability transient_local \
        >"${decision_log}" 2>&1 &
    local echo_pid=$!

    sleep 1

    ros2 run "${PACKAGE}" client_node \
        --ros-args \
        -p "request_id:=${request_id}"

    sleep 7

    kill "${echo_pid}" 2>/dev/null || true
    wait "${echo_pid}" 2>/dev/null || true
    kill -- "-${launch_pid}" 2>/dev/null || true
    wait "${launch_pid}" 2>/dev/null || true

    local failed=0

    if [[ -n "${expected_log}" ]]; then
        grep -q "${expected_log}" "${launch_log}" || {
            echo "FAIL: missing expected log: ${expected_log}"
            failed=1
        }
    fi

    if [[ "${expect_decision}" == "yes" ]]; then
        grep -q "request_id: ${request_id}" "${decision_log}" || {
            echo "FAIL: missing decision"
            failed=1
        }
        grep -q "committed: true" "${decision_log}" || {
            echo "FAIL: decision is not committed"
            failed=1
        }
    else
        if grep -q "committed: true" "${decision_log}"; then
            echo "FAIL: decision was published without quorum"
            failed=1
        fi
        grep -q "committed=False" "${launch_log}" || {
            echo "FAIL: no evidence replicas stayed uncommitted"
            failed=1
        }
    fi

    if grep -q "AGREEMENT VIOLATION" "${launch_log}"; then
        echo "FAIL: agreement violation"
        failed=1
    fi

    if [[ "${failed}" -eq 0 ]]; then
        echo "PASS: ${scenario}"
    else
        echo "Review ${launch_log} and ${decision_log}"
    fi

    return "${failed}"
}

overall=0

run_case \
    "pbft_simulator.launch.py" \
    "regression-normal" \
    "PBFT CONSENSUS CONFIRMED" \
    "yes" || overall=1

run_case \
    "pbft_silent_scenario.launch.py" \
    "regression-silent" \
    "SILENT BYZANTINE BEHAVIOR" \
    "yes" || overall=1

run_case \
    "pbft_bad_digest_scenario.launch.py" \
    "regression-bad-digest" \
    "BAD-DIGEST BYZANTINE BEHAVIOR" \
    "yes" || overall=1

run_case \
    "pbft_duplicate_scenario.launch.py" \
    "regression-duplicate" \
    "DUPLICATE BYZANTINE BEHAVIOR" \
    "yes" || overall=1

run_case \
    "pbft_equivocation_scenario.launch.py" \
    "regression-equivocation" \
    "EQUIVOCATION DETECTED" \
    "yes" || overall=1

run_case \
    "pbft_early_commit_scenario.launch.py" \
    "regression-early-commit" \
    "BUFFERED COMMIT WITHOUT QUORUM COUNT" \
    "yes" || overall=1

run_case \
    "pbft_insufficient_quorum_scenario.launch.py" \
    "regression-insufficient-quorum" \
    "SKIP-COMMIT BYZANTINE BEHAVIOR" \
    "no" || overall=1

exit "${overall}"
