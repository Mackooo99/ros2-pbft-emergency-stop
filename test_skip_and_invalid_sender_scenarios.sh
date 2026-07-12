#!/usr/bin/env bash
set -o pipefail

WORKSPACE="${HOME}/pbft_ws"
PACKAGE="pbft_emergency_stop_simulator"

start_case() {
    local scenario="$1"
    local request_id="$2"

    LAUNCH_LOG="${HOME}/${scenario%.launch.py}.log"
    DECISION_LOG="${HOME}/${scenario%.launch.py}_decision.log"

    echo
    echo "============================================================"
    echo "Running ${scenario}"
    echo "============================================================"

    cd "${WORKSPACE}" || return 1

    set +u
    source install/setup.bash
    set -u

    rm -f "${LAUNCH_LOG}" "${DECISION_LOG}"

    setsid ros2 launch "${PACKAGE}" "${scenario}" \
        >"${LAUNCH_LOG}" 2>&1 &
    LAUNCH_PID=$!

    sleep 3

    timeout 15 ros2 topic echo /pbft/decision \
        --once \
        --qos-reliability reliable \
        --qos-durability transient_local \
        >"${DECISION_LOG}" 2>&1 &
    ECHO_PID=$!

    sleep 1

    ros2 run "${PACKAGE}" client_node \
        --ros-args \
        -p "request_id:=${request_id}"

    wait "${ECHO_PID}" || true
    sleep 2

    kill -- "-${LAUNCH_PID}" 2>/dev/null || true
    wait "${LAUNCH_PID}" 2>/dev/null || true
}

check_decision() {
    local failed=0

    grep -q "committed: true" "${DECISION_LOG}" || {
        echo "FAIL: no committed decision"
        failed=1
    }

    grep -q "confirmation_count: 3" "${DECISION_LOG}" || {
        echo "FAIL: confirmation_count is not 3"
        failed=1
    }

    grep -q "required_confirmations: 3" "${DECISION_LOG}" || {
        echo "FAIL: required_confirmations is not 3"
        failed=1
    }

    grep -q -- "- 0" "${DECISION_LOG}" &&
    grep -q -- "- 1" "${DECISION_LOG}" &&
    grep -q -- "- 2" "${DECISION_LOG}" || {
        echo "FAIL: confirming replicas are not 0, 1, 2"
        failed=1
    }

    return "${failed}"
}

overall=0

# skip_prepare
start_case \
    "pbft_skip_prepare_scenario.launch.py" \
    "skip-prepare-test"

failed=0

grep -q "SKIP-PREPARE BYZANTINE BEHAVIOR" "${LAUNCH_LOG}" || {
    echo "FAIL: missing skip-PREPARE behavior log"
    failed=1
}

if grep -q "\[pbft_node_3\]: Published PREPARE" "${LAUNCH_LOG}"; then
    echo "FAIL: node 3 published PREPARE in skip_prepare scenario"
    failed=1
fi

grep -q "\[pbft_node_3\]: PREPARED:" "${LAUNCH_LOG}" || {
    echo "FAIL: node 3 did not later reach PREPARED"
    failed=1
}

grep -q "\[pbft_node_3\]: Published COMMIT:" "${LAUNCH_LOG}" || {
    echo "FAIL: node 3 did not publish COMMIT after PREPARED"
    failed=1
}

check_decision || failed=1

if [[ "${failed}" -eq 0 ]]; then
    echo "PASS: pbft_skip_prepare_scenario.launch.py"
else
    overall=1
    echo "Review:"
    echo "  ${LAUNCH_LOG}"
    echo "  ${DECISION_LOG}"
fi

# skip_commit
start_case \
    "pbft_skip_commit_scenario.launch.py" \
    "skip-commit-test"

failed=0

grep -q "SKIP-COMMIT BYZANTINE BEHAVIOR" "${LAUNCH_LOG}" || {
    echo "FAIL: missing skip-COMMIT behavior log"
    failed=1
}

grep -q "\[pbft_node_3\]: PREPARED:" "${LAUNCH_LOG}" || {
    echo "FAIL: node 3 did not reach PREPARED"
    failed=1
}

if grep -q "\[pbft_node_3\]: Published COMMIT:" "${LAUNCH_LOG}"; then
    echo "FAIL: node 3 published COMMIT in skip_commit scenario"
    failed=1
fi

check_decision || failed=1

if [[ "${failed}" -eq 0 ]]; then
    echo "PASS: pbft_skip_commit_scenario.launch.py"
else
    overall=1
    echo "Review:"
    echo "  ${LAUNCH_LOG}"
    echo "  ${DECISION_LOG}"
fi

# invalid_sender
start_case \
    "pbft_invalid_sender_scenario.launch.py" \
    "invalid-sender-test"

failed=0

grep -q "INVALID-SENDER BYZANTINE BEHAVIOR: published PREPARE" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: missing invalid-sender PREPARE log"
    failed=1
}

grep -q "Rejected PREPARE with an invalid sender_id: 4" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: invalid-sender PREPARE was not rejected"
    failed=1
}

grep -q "INVALID-SENDER BYZANTINE BEHAVIOR: published COMMIT" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: missing invalid-sender COMMIT log"
    failed=1
}

grep -q "Rejected COMMIT with an invalid sender_id: 4" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: invalid-sender COMMIT was not rejected"
    failed=1
}

check_decision || failed=1

if [[ "${failed}" -eq 0 ]]; then
    echo "PASS: pbft_invalid_sender_scenario.launch.py"
else
    overall=1
    echo "Review:"
    echo "  ${LAUNCH_LOG}"
    echo "  ${DECISION_LOG}"
fi

exit "${overall}"
