#!/bin/sh

[ $(whoami) == "root" ] || exit 1

go build || exit 1
pkill agentic_tcp
rm -f *.log
# rm *.log *.csv

test_duration=300


set -a && . ./.env && set +a

export TEST_MODE_ENABLED=1

export TEST_TRAFFIC_DELAY_INTERVAL="10ms"
export TEST_TRAFFIC_BITS_TO_SEND="9600"

profile() {

  export LLM_ENABLED=1

  echo "### RUNNING PROFILE A ###"
  date
  echo

  PROFILE_METRICS_NAME_PREFIX="profile_$1_with_llm_A_" GROQ_API_KEY=$GROQ_API_KEY_A ./agentic_tcp -listen :9001 -send :9002 &
  p1=$!

  PROFILE_METRICS_NAME_PREFIX="profile_$1_with_llm_B_" GROQ_API_KEY=$GROQ_API_KEY_B ./agentic_tcp -listen :9002 -send :9001 &
  p2=$!

  sleep $test_duration
  kill "$p1" "$p2" 2>/dev/null || true
  wait "$p1" "$p2" 2>/dev/null || true


  export LLM_ENABLED=0

  echo "### RUNNING PROFILE B ###"
  date
  echo


  PROFILE_METRICS_NAME_PREFIX="profile_$1_without_llm_A_" GROQ_API_KEY=$GROQ_API_KEY_A ./agentic_tcp -listen :9001 -send :9002 &
  p1=$!

  PROFILE_METRICS_NAME_PREFIX="profile_$1_without_llm_B_" GROQ_API_KEY=$GROQ_API_KEY_B ./agentic_tcp -listen :9002 -send :9001 &
  p2=$!

  sleep $test_duration
  kill "$p1" "$p2" 2>/dev/null || true
  wait "$p1" "$p2" 2>/dev/null || true

}

clear_tc_settings() {
  tc qdisc del dev lo root || true
}

clear_tc_settings
profile clean

clear_tc_settings
tc qdisc add dev lo root netem delay 80ms 20ms loss 1% reorder 3% || exit 1
profile mobile

clear_tc_settings
tc qdisc add dev lo root netem delay 300ms 50ms loss 0.5% || exit 1
profile satellite

clear_tc_settings
tc qdisc add dev lo root netem delay 20ms 5ms loss 0.1% || exit 1
profile wifi

# export LLM_INPUT_MODE=throughput_history
# clear_tc_settings
# tc qdisc add dev lo root netem delay 20ms 5ms loss 0.1% || exit 1
# profile wifi_throughput_history
# unset LLM_INPUT_MODE


clear_tc_settings
