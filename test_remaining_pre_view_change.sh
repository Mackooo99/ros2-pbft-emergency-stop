#!/usr/bin/env bash
set -o pipefail

WORKSPACE="${HOME}/pbft_ws"
PACKAGE="pbft_emergency_stop_simulator"

source_workspace() {
    cd "${WORKSPACE}" || return 1
    set +u
    source install/setup.bash
    set -u
}

stop_launch() {
    if [[ -n "${LAUNCH_PID:-}" ]]; then
        kill -- "-${LAUNCH_PID}" 2>/dev/null || true
        wait "${LAUNCH_PID}" 2>/dev/null || true
    fi
}

wait_for_log() {
    local file="$1"
    local pattern="$2"
    local timeout_sec="${3:-12}"
    local elapsed=0

    while (( elapsed < timeout_sec * 10 )); do
        if grep -q "${pattern}" "${file}" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
        elapsed=$((elapsed + 1))
    done

    return 1
}

start_scenario() {
    local scenario="$1"
    local case_name="$2"
    local decision_timeout="${3:-18}"

    LAUNCH_LOG="${HOME}/${case_name}.log"
    DECISION_LOG="${HOME}/${case_name}_decision.log"

    rm -f "${LAUNCH_LOG}" "${DECISION_LOG}"

    setsid ros2 launch "${PACKAGE}" "${scenario}" \
        >"${LAUNCH_LOG}" 2>&1 &
    LAUNCH_PID=$!

    sleep 3

    timeout "${decision_timeout}" ros2 topic echo /pbft/decision \
        --qos-reliability reliable \
        --qos-durability transient_local \
        >"${DECISION_LOG}" 2>&1 &
    ECHO_PID=$!

    sleep 1
}

send_request() {
    local request_id="$1"

    ros2 run "${PACKAGE}" client_node \
        --ros-args \
        -p "request_id:=${request_id}"
}

check_no_agreement_violation() {
    if grep -q "AGREEMENT VIOLATION" "${LAUNCH_LOG}"; then
        echo "FAIL: monitor reported AGREEMENT VIOLATION"
        return 1
    fi
    return 0
}

check_decision_for() {
    local request_id="$1"

    grep -q "request_id: ${request_id}" "${DECISION_LOG}" || {
        echo "FAIL: missing decision for ${request_id}"
        return 1
    }

    grep -q "committed: true" "${DECISION_LOG}" || {
        echo "FAIL: decision is not committed"
        return 1
    }

    grep -q "required_confirmations: 3" "${DECISION_LOG}" || {
        echo "FAIL: wrong decision threshold"
        return 1
    }

    return 0
}

source_workspace || exit 1
trap stop_launch EXIT
overall=0

echo
echo "============================================================"
echo "Running duplicate_request"
echo "============================================================"

start_scenario \
    "pbft_simulator.launch.py" \
    "pbft_duplicate_request_test" \
    12

send_request "duplicate-request-test"

wait_for_log \
    "${LAUNCH_LOG}" \
    "Published confirmed decision on /pbft/decision: request_id=duplicate-request-test" \
    10 || true

send_request "duplicate-request-test"
sleep 2

kill "${ECHO_PID}" 2>/dev/null || true
wait "${ECHO_PID}" 2>/dev/null || true
stop_launch
LAUNCH_PID=""

failed=0

[[ "$(grep -c "Accepted valid REQUEST: request_id=duplicate-request-test" "${LAUNCH_LOG}")" -eq 1 ]] || {
    echo "FAIL: duplicate request was accepted more than once"
    failed=1
}

grep -q "Duplicate REQUEST ignored: request_id=duplicate-request-test" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: duplicate request was not explicitly ignored"
    failed=1
}

[[ "$(grep -c "Published PRE-PREPARE:.*request_id=duplicate-request-test" "${LAUNCH_LOG}")" -eq 1 ]] || {
    echo "FAIL: duplicate request created another PRE-PREPARE"
    failed=1
}

[[ "$(grep -c "request_id: duplicate-request-test" "${DECISION_LOG}")" -eq 1 ]] || {
    echo "FAIL: duplicate request created zero or multiple decisions"
    failed=1
}

check_no_agreement_violation || failed=1

if [[ "${failed}" -eq 0 ]]; then
    echo "PASS: duplicate_request"
else
    overall=1
    echo "Review ${LAUNCH_LOG} and ${DECISION_LOG}"
fi

echo
echo "============================================================"
echo "Running multiple_requests"
echo "============================================================"

start_scenario \
    "pbft_simulator.launch.py" \
    "pbft_multiple_requests_test" \
    22

send_request "multi-request-A"

wait_for_log \
    "${LAUNCH_LOG}" \
    "Published confirmed decision on /pbft/decision: request_id=multi-request-A" \
    12 || {
    echo "FAIL: first request did not finish"
    overall=1
}

send_request "multi-request-B"

wait_for_log \
    "${LAUNCH_LOG}" \
    "Published confirmed decision on /pbft/decision: request_id=multi-request-B" \
    12 || {
    echo "FAIL: second request did not finish"
    overall=1
}

sleep 2
kill "${ECHO_PID}" 2>/dev/null || true
wait "${ECHO_PID}" 2>/dev/null || true
stop_launch
LAUNCH_PID=""

failed=0

grep -q "Accepted valid REQUEST: request_id=multi-request-A, assigned_sequence=1" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: request A was not assigned sequence 1"
    failed=1
}

grep -q "Accepted valid REQUEST: request_id=multi-request-B, assigned_sequence=2" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: request B was not assigned sequence 2"
    failed=1
}

check_decision_for "multi-request-A" || failed=1
check_decision_for "multi-request-B" || failed=1

[[ "$(grep -c "committed: true" "${DECISION_LOG}")" -eq 2 ]] || {
    echo "FAIL: expected exactly two committed decisions"
    failed=1
}

grep -q "sequence_number: 1" "${DECISION_LOG}" || {
    echo "FAIL: missing decision for sequence 1"
    failed=1
}

grep -q "sequence_number: 2" "${DECISION_LOG}" || {
    echo "FAIL: missing decision for sequence 2"
    failed=1
}

check_no_agreement_violation || failed=1

if [[ "${failed}" -eq 0 ]]; then
    echo "PASS: multiple_requests"
else
    overall=1
    echo "Review ${LAUNCH_LOG} and ${DECISION_LOG}"
fi

echo
echo "============================================================"
echo "Running delayed_prepare"
echo "============================================================"

start_scenario \
    "pbft_delayed_prepare_scenario.launch.py" \
    "pbft_delayed_prepare_test" \
    16

send_request "delayed-prepare-test"

wait_for_log \
    "${LAUNCH_LOG}" \
    "DELAYED-PREPARE BYZANTINE BEHAVIOR: published PREPARE after" \
    12 || true

sleep 1
kill "${ECHO_PID}" 2>/dev/null || true
wait "${ECHO_PID}" 2>/dev/null || true
stop_launch
LAUNCH_PID=""

failed=0

grep -q "DELAYED-PREPARE BYZANTINE BEHAVIOR: scheduled PREPARE" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: PREPARE delay was not scheduled"
    failed=1
}

grep -q "DELAYED-PREPARE BYZANTINE BEHAVIOR: published PREPARE after" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: delayed PREPARE was not published"
    failed=1
}

check_decision_for "delayed-prepare-test" || failed=1

decision_line="$(
    grep -n "Published confirmed decision on /pbft/decision: request_id=delayed-prepare-test" \
        "${LAUNCH_LOG}" | head -n 1 | cut -d: -f1
)"
delayed_line="$(
    grep -n "DELAYED-PREPARE BYZANTINE BEHAVIOR: published PREPARE after" \
        "${LAUNCH_LOG}" | head -n 1 | cut -d: -f1
)"

if [[ -z "${decision_line}" || -z "${delayed_line}" ||
      "${decision_line}" -ge "${delayed_line}" ]]; then
    echo "FAIL: consensus did not finish before delayed PREPARE"
    failed=1
fi

check_no_agreement_violation || failed=1

if [[ "${failed}" -eq 0 ]]; then
    echo "PASS: delayed_prepare"
else
    overall=1
    echo "Review ${LAUNCH_LOG} and ${DECISION_LOG}"
fi

echo
echo "============================================================"
echo "Running delayed_commit"
echo "============================================================"

start_scenario \
    "pbft_delayed_commit_scenario.launch.py" \
    "pbft_delayed_commit_test" \
    16

send_request "delayed-commit-test"

wait_for_log \
    "${LAUNCH_LOG}" \
    "DELAYED-COMMIT BYZANTINE BEHAVIOR: published COMMIT after" \
    12 || true

sleep 1
kill "${ECHO_PID}" 2>/dev/null || true
wait "${ECHO_PID}" 2>/dev/null || true
stop_launch
LAUNCH_PID=""

failed=0

grep -q "DELAYED-COMMIT BYZANTINE BEHAVIOR: scheduled COMMIT" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: COMMIT delay was not scheduled"
    failed=1
}

grep -q "DELAYED-COMMIT BYZANTINE BEHAVIOR: published COMMIT after" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: delayed COMMIT was not published"
    failed=1
}

check_decision_for "delayed-commit-test" || failed=1

decision_line="$(
    grep -n "Published confirmed decision on /pbft/decision: request_id=delayed-commit-test" \
        "${LAUNCH_LOG}" | head -n 1 | cut -d: -f1
)"
delayed_line="$(
    grep -n "DELAYED-COMMIT BYZANTINE BEHAVIOR: published COMMIT after" \
        "${LAUNCH_LOG}" | head -n 1 | cut -d: -f1
)"

if [[ -z "${decision_line}" || -z "${delayed_line}" ||
      "${decision_line}" -ge "${delayed_line}" ]]; then
    echo "FAIL: consensus did not finish before delayed COMMIT"
    failed=1
fi

check_no_agreement_violation || failed=1

if [[ "${failed}" -eq 0 ]]; then
    echo "PASS: delayed_commit"
else
    overall=1
    echo "Review ${LAUNCH_LOG} and ${DECISION_LOG}"
fi

echo
echo "============================================================"
echo "Running early_prepare"
echo "============================================================"

start_scenario \
    "pbft_simulator.launch.py" \
    "pbft_early_prepare_test" \
    14

request_id="early-prepare-test"
digest="$(
python3 - <<'PY'
import hashlib

request_id = "early-prepare-test"
canonical = (
    f"request_id={request_id};"
    "emergency_stop=1"
)
print(hashlib.sha256(canonical.encode("utf-8")).hexdigest())
PY
)"

ros2 topic pub --once \
    /pbft/prepare \
    pbft_emergency_stop_interfaces/msg/PBFTMessage \
    "{message_type: 2, sender_id: 3, recipient_id: -1, view: 0, sequence_number: 1, request_id: '${request_id}', request_digest: '${digest}', emergency_stop: true}" \
    >/dev/null 2>&1

sleep 1
send_request "${request_id}"

wait_for_log \
    "${LAUNCH_LOG}" \
    "Published confirmed decision on /pbft/decision: request_id=${request_id}" \
    12 || true

sleep 1
kill "${ECHO_PID}" 2>/dev/null || true
wait "${ECHO_PID}" 2>/dev/null || true
stop_launch
LAUNCH_PID=""

failed=0

grep -q "Buffered early PREPARE: sender=3, view=0, sequence=1" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: early PREPARE was not buffered"
    failed=1
}

grep -q "Accepted PREPARE: sender=3, view=0, sequence=1" \
    "${LAUNCH_LOG}" || {
    echo "FAIL: buffered PREPARE was not processed later"
    failed=1
}

check_decision_for "${request_id}" || failed=1
check_no_agreement_violation || failed=1

if [[ "${failed}" -eq 0 ]]; then
    echo "PASS: early_prepare"
else
    overall=1
    echo "Review ${LAUNCH_LOG} and ${DECISION_LOG}"
fi

exit "${overall}"
