#!/bin/sh

go build || exit 1
pkill agentic_tcp
rm *.log
# rm *.log *.csv

test_duration=300

export TEST_TRAFFIC_DELAY_INTERVAL="1ms"
export TEST_TRAFFIC_BITS_TO_SEND="9600"
export PROFILE_METRICS_NAME_PREFIX="profile_with_retransmission_ratio_"

echo "### RUNNING PROFILE 1 ###"


./agentic_tcp -listen :9001 -send :9002 &
./agentic_tcp -listen :9002 -send :9001 &

sleep $test_duration && pkill agentic_tcp

exit

export TEST_TRAFFIC_DELAY_INTERVAL="10ms"
export TEST_TRAFFIC_BITS_TO_SEND="9600"
export PROFILE_METRICS_NAME_PREFIX="profile_long_delay_"

echo "### RUNNING PROFILE 2 ###"

./agentic_tcp -listen :9001 -send :9002 &
./agentic_tcp -listen :9002 -send :9001 &

sleep $test_duration && pkill agentic_tcp
