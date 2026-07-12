#!/usr/bin/env bash
set -o pipefail

cd "${HOME}/pbft_ws" || exit 1

scripts=(
    "./test_existing_pbft_regression.sh"
    "./test_three_validation_scenarios.sh"
    "./test_skip_and_invalid_sender_scenarios.sh"
    "./test_remaining_pre_view_change.sh"
)

overall=0

for script in "${scripts[@]}"; do
    echo
    echo "################################################################"
    echo "Running ${script}"
    echo "################################################################"

    if [[ ! -x "${script}" ]]; then
        echo "FAIL: missing or non-executable script ${script}"
        overall=1
        continue
    fi

    "${script}" || overall=1
done

echo
echo "################################################################"

if [[ "${overall}" -eq 0 ]]; then
    echo "ALL PRE-VIEW-CHANGE REGRESSION TESTS PASSED"
else
    echo "PRE-VIEW-CHANGE REGRESSION HAS FAILURES"
fi

echo "################################################################"

exit "${overall}"
