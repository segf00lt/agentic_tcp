#!/usr/bin/env bash
set -euo pipefail

DEV="${DEV:-lo}"

usage() {
  cat <<'EOF'
Usage:
  netem.sh status
  netem.sh off
  netem.sh preset <clean|wifi|mobile|satellite|harsh>
  netem.sh custom [options]

Options for custom:
  --delay MS          Base delay in milliseconds
  --jitter MS         Jitter in milliseconds
  --loss PERCENT      Packet loss percentage
  --rate RATE         Bandwidth limit, e.g. 10mbit, 500kbit
  --reorder PERCENT   Packet reordering percentage
  --duplicate PERCENT Packet duplication percentage
  --corrupt PERCENT   Packet corruption percentage

Examples:
  sudo ./netem.sh preset wifi
  sudo ./netem.sh preset mobile
  sudo ./netem.sh custom --delay 50 --jitter 10 --loss 1 --rate 10mbit
  sudo ./netem.sh off

Notes:
  - This script targets interface: $DEV
  - "off" deletes the root qdisc and restores normal behavior.
EOF
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "Please run as root (for example: sudo $0 ...)" >&2
    exit 1
  fi
}

show_status() {
  tc qdisc show dev "$DEV" || true
}

disable_all() {
  tc qdisc del dev "$DEV" root 2>/dev/null || true
  echo "Disabled shaping on $DEV"
}

apply_clean() {
  disable_all
}

apply_preset() {
  local preset="$1"

  disable_all

  case "$preset" in
    wifi)
      tc qdisc add dev "$DEV" root netem delay 20ms 5ms loss 0.1%
      ;;
    mobile)
      tc qdisc add dev "$DEV" root netem delay 80ms 20ms loss 1% reorder 2%
      ;;
    satellite)
      tc qdisc add dev "$DEV" root netem delay 300ms 50ms loss 0.5%
      ;;
    harsh)
      tc qdisc add dev "$DEV" root netem delay 100ms 30ms loss 5% reorder 10% duplicate 1%
      ;;
    clean)
      apply_clean
      ;;
    *)
      echo "Unknown preset: $preset" >&2
      usage
      exit 1
      ;;
  esac

  echo "Applied preset '$preset' on $DEV"
}

apply_custom() {
  shift || true

  local delay=""
  local jitter=""
  local loss=""
  local rate=""
  local reorder=""
  local duplicate=""
  local corrupt=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --delay)     delay="${2:-}"; shift 2 ;;
      --jitter)    jitter="${2:-}"; shift 2 ;;
      --loss)      loss="${2:-}"; shift 2 ;;
      --rate)      rate="${2:-}"; shift 2 ;;
      --reorder)   reorder="${2:-}"; shift 2 ;;
      --duplicate) duplicate="${2:-}"; shift 2 ;;
      --corrupt)   corrupt="${2:-}"; shift 2 ;;
      *)
        echo "Unknown option: $1" >&2
        usage
        exit 1
        ;;
    esac
  done

  disable_all

  local args=()
  if [[ -n "$delay" ]]; then
    if [[ -n "$jitter" ]]; then
      args+=(delay "${delay}ms" "${jitter}ms")
    else
      args+=(delay "${delay}ms")
    fi
  fi
  if [[ -n "$loss" ]]; then
    args+=(loss "${loss}%")
  fi
  if [[ -n "$reorder" ]]; then
    args+=(reorder "${reorder}%")
  fi
  if [[ -n "$duplicate" ]]; then
    args+=(duplicate "${duplicate}%")
  fi
  if [[ -n "$corrupt" ]]; then
    args+=(corrupt "${corrupt}%")
  fi

  if [[ -n "$rate" ]]; then
    # Bandwidth limiting is typically done with tbf or htb, not netem alone.
    # For a simple localhost test, we apply tbf first, then netem under it.
    # If you want a simpler setup, use "rate" alone and skip the delay/loss.
    tc qdisc add dev "$DEV" root handle 1: tbf rate "$rate" burst 32kbit latency 400ms
    if [[ ${#args[@]} -gt 0 ]]; then
      tc qdisc add dev "$DEV" parent 1:1 handle 10: netem "${args[@]}"
    fi
  else
    tc qdisc add dev "$DEV" root netem "${args[@]}"
  fi

  echo "Applied custom settings on $DEV"
}

main() {
  require_root

  if [[ $# -lt 1 ]]; then
    usage
    exit 1
  fi

  case "$1" in
    status)
      show_status
      ;;
    off)
      disable_all
      ;;
    preset)
      [[ $# -ge 2 ]] || { usage; exit 1; }
      apply_preset "$2"
      ;;
    clean)
      apply_clean
      ;;
    custom)
      apply_custom "$@"
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"