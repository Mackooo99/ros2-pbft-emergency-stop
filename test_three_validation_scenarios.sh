#!/usr/bin/env bash
set -o pipefail

WORKSPACE="${HOME}/pbft_ws"
PACKAGE="pbft_emergency_stop_simulator"

run_case() {
    local scenario="$1"
    local request_id="$2"
    local expected_fault_log="$3"
    local expected_rejection_log="$4"

    local launch_log="${HOME}/${scenario%.launch.py}.log"
    local decision_log="${HOME}/${scenario%.launch.py}_decision.log"

    echo
    echo "============================================================"
    echo "Running ${scenario}"
    echo "============================================================"

    cd "${WORKSPACE}" || return 1
    set +u
    source install/setup.bash
    set -u

    rm -f "${launch_log}" "${decision_log}"

    setsid ros2 launch "${PACKAGE}" "${scenario}" \
        >"${launch_log}" 2>&1 &
    local launch_pid=$!

    sleep 3

    timeout 15 ros2 topic echo /pbft/decision \
        --once \
        --qos-reliability reliable \
        --qos-durability transient_local \
        >"${decision_log}" 2>&1 &
    local echo_pid=$!

    sleep 1

    ros2 run "${PACKAGE}" client_node \
        --ros-args \
        -p "request_id:=${request_id}"

    wait "${echo_pid}" || true
    sleep 2

    kill -- "-${launch_pid}" 2>/dev/null || true
    wait "${launch_pid}" 2>/dev/null || true

    local failed=0

    grep -q "${expected_fault_log}" "${launch_log}" || {
        echo "FAIL: missing Byzantine behavior log"
        failed=1
    }

    grep -q "${expected_rejection_log}" "${launch_log}" || {
        echo "FAIL: missing rejection log"
        failed=1
    }

    grep -q "committed: true" "${decision_log}" || {
        echo "FAIL: no committed decision"
        failed=1
    }

    grep -q "confirmation_count: 3" "${decision_log}" || {
        echo "FAIL: confirmation_count is not 3"
        failed=1
    }

    grep -q -- "- 0" "${decision_log}" &&
    grep -q -- "- 1" "${decision_log}" &&
    grep -q -- "- 2" "${decision_log}" || {
        echo "FAIL: confirming replicas are not 0, 1, 2"
        failed=1
    }

    if [[ "${failed}" -eq 0 ]]; then
        echo "PASS: ${scenario}"
    else
        echo "Review:"
        echo "  ${launch_log}"
        echo "  ${decision_log}"
    fi

    return "${failed}"
}

overall=0

run_case \
    "pbft_wrong_sequence_scenario.launch.py" \
    "wrong-sequence-test" \
    "WRONG-SEQUENCE BYZANTINE BEHAVIOR" \
    "Rejected PREPARE with sequence_number=0" || overall=1

run_case \
    "pbft_wrong_view_scenario.launch.py" \
    "wrong-view-test" \
    "WRONG-VIEW BYZANTINE BEHAVIOR" \
    "Rejected PREPARE with an invalid view" || overall=1

run_case \
    "pbft_wrong_value_scenario.launch.py" \
    "wrong-value-test" \
    "WRONG-VALUE BYZANTINE BEHAVIOR" \
    "Rejected PREPARE with emergency_stop=false" || overall=1

exit "${overall}"
