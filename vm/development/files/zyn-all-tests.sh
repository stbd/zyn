#!/usr/bin/env bash

set -euo pipefail

cmd_system_tests=zyn-system-tests-slow.sh
cmd_static_analysis=zyn-static-analysis.sh
cmd_build=zyn-build.sh
cmd_unittests=zyn-unittests.sh
result=0

"$cmd_build"
result_build=$?

if [ "$result_build" -ne 0 ]; then
    echo "Build failed, replicate the problem with:"
    echo "$cmd_build"
    return 1
fi

result_static_analysis=0
result_unittests=0
result_system_tests=0
"$cmd_static_analysis" || result_static_analysis=$?
"$cmd_unittests" || result_unittests=$?
"$cmd_system_tests" || result_system_tests=$?

if [ "$result_static_analysis" -ne 0 ]; then
    echo "Static code analysis failed, replicate the problem with:"
    echo "$cmd_static_analysis"
    result=1
fi

if [ "$result_unittests" -ne 0 ]; then
    echo "Unittests failed, replicate the problem with:"
    echo "$cmd_unittests"
    result=1
fi

if [ "$result_system_tests" -ne 0 ]; then
    echo "System tests failed, replicate the problem with:"
    echo "$cmd_system_tests"
    result=1
fi

if [ "$result" -eq 0 ]; then
    echo
    echo "--------------------------------"
    echo "All tests completed successfully"
fi

exit "$result"
