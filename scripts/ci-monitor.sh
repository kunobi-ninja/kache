#!/usr/bin/env bash
set -euo pipefail

# ─── kache CI Monitor ────────────────────────────────────────────────────────
# Extract kache build cache metrics from GitHub Actions runs.
# Data comes from run metadata, job step info, and (optionally) job logs.
# ─────────────────────────────────────────────────────────────────────────────

readonly VERSION="0.1.0"
readonly DEFAULT_REPO="Zondax/kunobi-frontend"
readonly DEFAULT_LIMIT=10
readonly MAX_PARALLEL=5

# ─── Color / formatting ─────────────────────────────────────────────────────

setup_colors() {
  if [[ -n "${NO_COLOR:-}" ]] || [[ ! -t 1 ]]; then
    BOLD="" DIM="" RESET="" RED="" GREEN="" YELLOW="" CYAN="" BLUE=""
  else
    BOLD=$'\033[1m'  DIM=$'\033[2m'  RESET=$'\033[0m'
    RED=$'\033[31m'  GREEN=$'\033[32m'  YELLOW=$'\033[33m'
    CYAN=$'\033[36m'  BLUE=$'\033[34m'
  fi
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

die()  { echo "${RED}error:${RESET} $*" >&2; exit 1; }
log()  { echo "${DIM}$*${RESET}" >&2; }
tick() { echo "${GREEN}✓${RESET} $*" >&2; }

usage() {
  cat <<EOF
${BOLD}kache CI Monitor${RESET} v${VERSION}

Extract kache build cache metrics from GitHub Actions runs.

${BOLD}USAGE${RESET}
  $(basename "$0") [OPTIONS]

${BOLD}OPTIONS${RESET}
  --repo OWNER/REPO   Repository (default: ${DEFAULT_REPO})
  --limit N           Number of recent runs (default: ${DEFAULT_LIMIT})
  --pr N              Show stats for a single PR
  --workflow NAME     Filter by workflow name (substring match)
  --logs              Download job logs for extended data (slower)
  --save-logs DIR     Download kache job logs and save to DIR for inspection
  --json              Output raw JSON instead of table
  --help              Show this help

${BOLD}EXAMPLES${RESET}
  $(basename "$0")                                  # last 10 runs on ${DEFAULT_REPO}
  $(basename "$0") --repo Zondax/other --limit 20   # different repo
  $(basename "$0") --pr 750                         # single PR stats
  $(basename "$0") --logs                           # extended data from logs
  $(basename "$0") --pr 750 --save-logs ./ci-logs   # save logs for inspection
EOF
  exit 0
}

dependency_check() {
  local missing=()
  command -v gh  >/dev/null 2>&1 || missing+=(gh)
  command -v jq  >/dev/null 2>&1 || missing+=(jq)
  if (( ${#missing[@]} > 0 )); then
    die "missing required tools: ${missing[*]}"
  fi
  # Verify gh auth
  gh auth status >/dev/null 2>&1 || die "gh not authenticated — run 'gh auth login'"
}

# Portable epoch conversion (GNU vs BSD date)
if date --version >/dev/null 2>&1; then
  epoch() { date -d "$1" +%s 2>/dev/null || echo 0; }
else
  epoch() { date -jf "%Y-%m-%dT%H:%M:%SZ" "$1" +%s 2>/dev/null || echo 0; }
fi

# ─── Initialize colors early (needed by usage/die) ──────────────────────────
setup_colors

# ─── Argument parsing ────────────────────────────────────────────────────────

REPO="$DEFAULT_REPO"
LIMIT="$DEFAULT_LIMIT"
PR_NUMBER=""
WORKFLOW_FILTER=""
USE_LOGS=false
SAVE_LOGS_DIR=""
JSON_OUTPUT=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)      REPO="${2:?--repo requires OWNER/REPO}"; shift 2 ;;
    --limit)     LIMIT="${2:?--limit requires a number}"; shift 2 ;;
    --pr)        PR_NUMBER="${2:?--pr requires a number}"; shift 2 ;;
    --workflow)  WORKFLOW_FILTER="${2:?--workflow requires a name}"; shift 2 ;;
    --logs)      USE_LOGS=true; shift ;;
    --save-logs) SAVE_LOGS_DIR="${2:?--save-logs requires a directory}"; USE_LOGS=true; shift 2 ;;
    --json)      JSON_OUTPUT=true; shift ;;
    --help|-h)   usage ;;
    --version)   echo "v${VERSION}"; exit 0 ;;
    *)           die "unknown option: $1 (use --help)" ;;
  esac
done

# ─── Temp directory with cleanup ─────────────────────────────────────────────

TMPDIR_WORK=$(mktemp -d)
trap 'rm -rf "$TMPDIR_WORK"' EXIT

# ─── Phase 1: List runs ─────────────────────────────────────────────────────

fetch_runs() {
  # Fetch more than needed, then filter — ensures we get enough useful runs.
  # workflow_run events are secondary triggers that never build Rust.
  local fetch_count=$(( LIMIT * 3 ))  # overfetch to compensate for filtering
  (( fetch_count < 30 )) && fetch_count=30

  local endpoint="repos/${REPO}/actions/runs?per_page=${fetch_count}&exclude_pull_requests=false"
  if [[ -n "$PR_NUMBER" ]]; then
    endpoint="repos/${REPO}/actions/runs?per_page=${fetch_count}"
  fi

  log "Fetching runs from ${REPO}..."

  gh api "$endpoint" --jq '.workflow_runs' > "$TMPDIR_WORK/runs_raw.json"

  # Filter: exclude workflow_run events (secondary triggers, never have kache)
  # Then apply PR/workflow filters and limit
  if [[ -n "$PR_NUMBER" ]]; then
    jq --argjson pr "$PR_NUMBER" \
      '[.[] | select(.event != "workflow_run") | select(.pull_requests[]?.number == $pr)]' \
      "$TMPDIR_WORK/runs_raw.json" > "$TMPDIR_WORK/runs.json"
  elif [[ -n "$WORKFLOW_FILTER" ]]; then
    jq --arg wf "$WORKFLOW_FILTER" \
      '[.[] | select(.event != "workflow_run") | select(.name | test($wf; "i"))]' \
      "$TMPDIR_WORK/runs_raw.json" > "$TMPDIR_WORK/runs.json"
  else
    jq '[.[] | select(.event != "workflow_run")]' \
      "$TMPDIR_WORK/runs_raw.json" > "$TMPDIR_WORK/runs.json"
  fi

  # Apply limit
  jq --argjson n "$LIMIT" '.[:$n]' "$TMPDIR_WORK/runs.json" > "$TMPDIR_WORK/runs_limited.json"
  mv "$TMPDIR_WORK/runs_limited.json" "$TMPDIR_WORK/runs.json"

  local count
  count=$(jq 'length' "$TMPDIR_WORK/runs.json")
  if [[ "$count" == "0" ]]; then
    die "no runs found (after filtering out workflow_run events)"
  fi

  # Extract run summaries
  jq -r '.[] | [
    .id,
    .head_branch,
    .event,
    (.conclusion // "running"),
    .run_started_at,
    .updated_at,
    (.pull_requests[0].number // ""),
    .name
  ] | @tsv' "$TMPDIR_WORK/runs.json" > "$TMPDIR_WORK/run_list.tsv"

  tick "$(wc -l < "$TMPDIR_WORK/run_list.tsv" | tr -d ' ') runs fetched"
}

# ─── Phase 2: Get job info per run (parallel) ───────────────────────────────

fetch_jobs_for_run() {
  local run_id="$1"
  local outfile="$TMPDIR_WORK/jobs_${run_id}.json"

  gh run view "$run_id" --repo "$REPO" --json jobs --jq '.jobs' > "$outfile" 2>/dev/null || echo "[]" > "$outfile"
}

fetch_all_jobs() {
  log "Fetching job details..."
  local pids=()
  local count=0

  while IFS=$'\t' read -r run_id _branch _event conclusion _rest; do
    # Skip fetching jobs for runs that can't have useful data
    if [[ "$conclusion" == "skipped" ]]; then
      echo "[]" > "$TMPDIR_WORK/jobs_${run_id}.json"
      continue
    fi

    fetch_jobs_for_run "$run_id" &
    pids+=($!)
    count=$((count + 1))

    # Throttle parallelism
    if (( count % MAX_PARALLEL == 0 )); then
      for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
      pids=()
    fi
  done < "$TMPDIR_WORK/run_list.tsv"

  # Wait for remaining
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  tick "Job details fetched for $count runs"
}

# Analyze jobs for a run: detect kache, count kache jobs, get timing
analyze_run_jobs() {
  local run_id="$1"
  local jobfile="$TMPDIR_WORK/jobs_${run_id}.json"

  if [[ ! -f "$jobfile" ]] || [[ "$(cat "$jobfile")" == "[]" ]]; then
    echo "0	0	-	-"
    return
  fi

  jq -r '
    def has_kache: .steps[]? | select(.name == "Setup kache" and .conclusion == "success");
    def kache_jobs: [.[] | select(has_kache)];
    def total_jobs: length;
    (kache_jobs | length) as $kj |
    total_jobs as $tj |
    (kache_jobs | [.[].databaseId] | join(",")) as $job_ids |
    (kache_jobs | map(.name) | join(", ")) as $job_names |
    "\($kj)\t\($tj)\t\($job_ids)\t\($job_names)"
  ' "$jobfile"
}

# ─── Phase 3: Fetch job logs (only with --logs) ─────────────────────────────

fetch_job_log() {
  local job_id="$1"
  local run_id="$2"
  local outfile="$TMPDIR_WORK/log_${run_id}_${job_id}.txt"

  gh api "repos/${REPO}/actions/jobs/${job_id}/logs" > "$outfile" 2>/dev/null || echo "" > "$outfile"
}

parse_job_log() {
  local run_id="$1"
  local job_ids="$2"
  local outfile="$TMPDIR_WORK/logstats_${run_id}.json"

  if [[ "$job_ids" == "-" ]] || [[ -z "$job_ids" ]]; then
    echo '{}' > "$outfile"
    return
  fi

  local version="" prefetch_total=0 manifest_entries=0 errors=0 warnings=0

  IFS=',' read -ra ids <<< "$job_ids"
  for jid in "${ids[@]}"; do
    local logfile="$TMPDIR_WORK/log_${run_id}_${jid}.txt"
    [[ -f "$logfile" ]] || continue

    # Extract kache version (first match)
    if [[ -z "$version" ]]; then
      version=$(grep -oE 'Using kache v[0-9]+\.[0-9]+\.[0-9]+' "$logfile" 2>/dev/null | head -1 | sed 's/Using kache //' || true)
    fi

    # Extract total prefetched crates (sum all "sending prefetch hint for N" lines)
    local pf_crates
    pf_crates=$(grep -oE 'sending prefetch hint for [0-9]+' "$logfile" 2>/dev/null \
      | awk '{s+=$NF} END {print s+0}') || true
    pf_crates=${pf_crates:-0}
    prefetch_total=$((prefetch_total + pf_crates))

    # Manifest entries
    local me
    me=$(grep -oE 'Saved manifest: [0-9]+ entries' "$logfile" 2>/dev/null \
      | head -1 | grep -oE '[0-9]+') || true
    me=${me:-0}
    if (( me > manifest_entries )); then
      manifest_entries=$me
    fi

    # Count errors and warnings from kache lines
    local e w
    e=$(grep -ciE 'ERROR.*kache|kache.*error' "$logfile" 2>/dev/null) || true
    e=${e:-0}
    w=$(grep -ciE 'WARN.*kache|kache.*warn' "$logfile" 2>/dev/null) || true
    w=${w:-0}
    errors=$((errors + e))
    warnings=$((warnings + w))
  done

  jq -n \
    --arg version "$version" \
    --argjson prefetch "$prefetch_total" \
    --argjson manifest "$manifest_entries" \
    --argjson errors "$errors" \
    --argjson warnings "$warnings" \
    '{version: $version, prefetch_crates: $prefetch, manifest_entries: $manifest, errors: $errors, warnings: $warnings}'  > "$outfile"
}

fetch_all_logs() {
  log "Downloading kache job logs..."
  local pids=()
  local count=0

  while IFS=$'\t' read -r run_id _branch _event _conclusion _rest; do
    local analysis
    analysis=$(analyze_run_jobs "$run_id")
    local kache_count job_ids
    kache_count=$(echo "$analysis" | cut -f1)
    job_ids=$(echo "$analysis" | cut -f3)

    if (( kache_count > 0 )) && [[ "$job_ids" != "-" ]]; then
      IFS=',' read -ra ids <<< "$job_ids"
      for jid in "${ids[@]}"; do
        fetch_job_log "$jid" "$run_id" &
        pids+=($!)
        count=$((count + 1))

        if (( count % MAX_PARALLEL == 0 )); then
          for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
          pids=()
        fi
      done
    fi
  done < "$TMPDIR_WORK/run_list.tsv"

  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  tick "$count job logs downloaded"

  # Parse all logs
  while IFS=$'\t' read -r run_id _rest; do
    local analysis
    analysis=$(analyze_run_jobs "$run_id")
    local job_ids
    job_ids=$(echo "$analysis" | cut -f3)
    parse_job_log "$run_id" "$job_ids"
  done < "$TMPDIR_WORK/run_list.tsv"

  # Save logs to directory if requested
  if [[ -n "$SAVE_LOGS_DIR" ]]; then
    mkdir -p "$SAVE_LOGS_DIR"
    local saved=0
    while IFS=$'\t' read -r run_id _rest; do
      local jobfile="$TMPDIR_WORK/jobs_${run_id}.json"
      [[ -f "$jobfile" ]] || continue

      # Get kache job IDs and names from the JSON directly
      local pairs
      pairs=$(jq -r '
        [.[] | select(.steps[]? | select(.name == "Setup kache" and .conclusion == "success"))]
        | .[] | "\(.databaseId)\t\(.name)"
      ' "$jobfile" 2>/dev/null) || continue
      [[ -z "$pairs" ]] && continue

      while IFS=$'\t' read -r jid jname; do
        local src="$TMPDIR_WORK/log_${run_id}_${jid}.txt"
        [[ -f "$src" ]] || continue
        local safe_name
        safe_name=$(echo "$jname" | tr ' /:' '_' | tr -cd '[:alnum:]_.-')
        local dest="${SAVE_LOGS_DIR}/${run_id}_${safe_name}.log"
        # Avoid collisions for jobs with the same name (e.g. matrix shards)
        if [[ -f "$dest" ]]; then
          dest="${SAVE_LOGS_DIR}/${run_id}_${safe_name}_${jid}.log"
        fi
        cp "$src" "$dest"
        saved=$((saved + 1))
      done <<< "$pairs"
    done < "$TMPDIR_WORK/run_list.tsv"
    tick "$saved log files saved to ${SAVE_LOGS_DIR}/"
  fi
}

# ─── Phase 4: Format and print table ────────────────────────────────────────

print_table() {
  local total_runs=0
  local kache_runs=0

  # Header
  if [[ -n "$PR_NUMBER" ]]; then
    echo "${BOLD}kache CI Monitor${RESET} — ${CYAN}${REPO}${RESET} (PR #${PR_NUMBER})"
  else
    echo "${BOLD}kache CI Monitor${RESET} — ${CYAN}${REPO}${RESET} (last ${LIMIT} runs)"
  fi
  echo ""

  # Table header
  if $USE_LOGS; then
    printf "${BOLD}%-14s %-20s %-6s %-8s %-8s %-8s %-10s %-10s %-6s %-6s %s${RESET}\n" \
      "Run" "Branch" "Event" "Status" "kache" "Version" "Prefetch" "Manifest" "Err" "Warn" "Duration"
  else
    printf "${BOLD}%-14s %-20s %-6s %-8s %-8s %s${RESET}\n" \
      "Run" "Branch" "Event" "Status" "kache" "Duration"
  fi

  # Table rows
  while IFS=$'\t' read -r run_id branch event conclusion started_at updated_at pr_num workflow_name; do
    total_runs=$((total_runs + 1))

    # Compute duration
    local duration="-"
    if [[ "$started_at" != "null" ]] && [[ "$updated_at" != "null" ]] && [[ -n "$started_at" ]] && [[ -n "$updated_at" ]]; then
      local s_epoch e_epoch
      s_epoch=$(epoch "$started_at")
      e_epoch=$(epoch "$updated_at")
      if (( s_epoch > 0 && e_epoch > 0 )); then
        local diff=$((e_epoch - s_epoch))
        if (( diff >= 60 )); then
          duration="$((diff / 60))m$((diff % 60))s"
        else
          duration="${diff}s"
        fi
      fi
    fi

    # Job analysis
    local analysis
    analysis=$(analyze_run_jobs "$run_id")
    local kache_count total_jobs job_ids job_names
    kache_count=$(echo "$analysis" | cut -f1)
    total_jobs=$(echo "$analysis" | cut -f2)
    job_ids=$(echo "$analysis" | cut -f3)
    job_names=$(echo "$analysis" | cut -f4-)

    local kache_str
    if (( kache_count > 0 )); then
      kache_str="${GREEN}${kache_count}/${total_jobs}${RESET}"
      kache_runs=$((kache_runs + 1))
    else
      kache_str="${DIM}0/${total_jobs}${RESET}"
    fi

    # Status formatting
    local status_str
    case "$conclusion" in
      success)   status_str="${GREEN}pass${RESET}" ;;
      failure)   status_str="${RED}fail${RESET}" ;;
      cancelled) status_str="${YELLOW}cancel${RESET}" ;;
      skipped)   status_str="${DIM}skip${RESET}" ;;
      running)   status_str="${BLUE}run...${RESET}" ;;
      *)         status_str="${DIM}${conclusion}${RESET}" ;;
    esac

    # Truncate branch name
    local branch_display
    if (( ${#branch} > 20 )); then
      branch_display="${branch:0:17}..."
    else
      branch_display="$branch"
    fi

    # Event abbreviation
    local event_short
    case "$event" in
      pull_request) event_short="pr" ;;
      workflow_run) event_short="wflow" ;;
      *)            event_short="$event" ;;
    esac

    if $USE_LOGS; then
      local logstats="$TMPDIR_WORK/logstats_${run_id}.json"
      local ver="-" pf="-" mf="-" errs="-" warns="-"
      if [[ -f "$logstats" ]] && [[ "$(cat "$logstats")" != "{}" ]]; then
        ver=$(jq -r '.version // "-"' "$logstats")
        pf=$(jq -r '.prefetch_crates // 0' "$logstats")
        mf=$(jq -r '.manifest_entries // 0' "$logstats")
        errs=$(jq -r '.errors // 0' "$logstats")
        warns=$(jq -r '.warnings // 0' "$logstats")
        [[ "$pf" == "0" ]] && pf="-"
        [[ "$mf" == "0" ]] && mf="-"
        # Color errors/warnings
        if [[ "$errs" != "0" ]]; then
          errs="${RED}${errs}${RESET}"
        else
          errs="-"
        fi
        if [[ "$warns" != "0" ]]; then
          warns="${YELLOW}${warns}${RESET}"
        else
          warns="-"
        fi
      fi
      printf "%-14s %-20s %-6s %-8s %-8s %-8s %-10s %-10s %-6s %-6s %s\n" \
        "$run_id" "$branch_display" "$event_short" "$status_str" "$kache_str" "$ver" "$pf" "$mf" "$errs" "$warns" "$duration"
    else
      printf "%-14s %-20s %-6s %-8s %-8s %s\n" \
        "$run_id" "$branch_display" "$event_short" "$status_str" "$kache_str" "$duration"
    fi

  done < "$TMPDIR_WORK/run_list.tsv"

  # Summary
  echo ""
  echo "${BOLD}Summary:${RESET} ${kache_runs}/${total_runs} runs use kache"

  # List unique kache job names seen
  if $USE_LOGS; then
    echo ""
    echo "${DIM}Tip: Stats (hit rate, local/remote hits, misses) are in the GitHub Actions job summary tab for each run.${RESET}"
  fi
}

# ─── JSON output mode ────────────────────────────────────────────────────────

print_json() {
  local results="[]"

  while IFS=$'\t' read -r run_id branch event conclusion started_at updated_at pr_num workflow_name; do
    local duration_s=0
    if [[ "$started_at" != "null" ]] && [[ "$updated_at" != "null" ]]; then
      local s_epoch e_epoch
      s_epoch=$(epoch "$started_at")
      e_epoch=$(epoch "$updated_at")
      if (( s_epoch > 0 && e_epoch > 0 )); then
        duration_s=$((e_epoch - s_epoch))
      fi
    fi

    local analysis
    analysis=$(analyze_run_jobs "$run_id")
    local kache_count total_jobs job_ids job_names
    kache_count=$(echo "$analysis" | cut -f1)
    total_jobs=$(echo "$analysis" | cut -f2)
    job_ids=$(echo "$analysis" | cut -f3)
    job_names=$(echo "$analysis" | cut -f4-)

    local logstats='{}'
    if $USE_LOGS && [[ -f "$TMPDIR_WORK/logstats_${run_id}.json" ]]; then
      logstats=$(cat "$TMPDIR_WORK/logstats_${run_id}.json")
    fi

    local entry
    entry=$(jq -n \
      --arg run_id "$run_id" \
      --arg branch "$branch" \
      --arg event "$event" \
      --arg conclusion "$conclusion" \
      --arg started_at "$started_at" \
      --arg updated_at "$updated_at" \
      --arg pr_num "$pr_num" \
      --arg workflow_name "$workflow_name" \
      --argjson duration_s "$duration_s" \
      --argjson kache_jobs "$kache_count" \
      --argjson total_jobs "$total_jobs" \
      --arg kache_job_names "$job_names" \
      --argjson log_stats "$logstats" \
      '{
        run_id: $run_id,
        branch: $branch,
        event: $event,
        conclusion: $conclusion,
        started_at: $started_at,
        updated_at: $updated_at,
        pr_number: (if $pr_num == "" then null else ($pr_num | tonumber) end),
        workflow: $workflow_name,
        duration_s: $duration_s,
        kache_jobs: $kache_jobs,
        total_jobs: $total_jobs,
        kache_job_names: $kache_job_names,
        log_stats: (if $log_stats == {} then null else $log_stats end)
      }')

    results=$(echo "$results" | jq --argjson e "$entry" '. + [$e]')
  done < "$TMPDIR_WORK/run_list.tsv"

  echo "$results" | jq '.'
}

# ─── Main ────────────────────────────────────────────────────────────────────

main() {
  setup_colors
  dependency_check

  fetch_runs
  fetch_all_jobs

  if $USE_LOGS; then
    fetch_all_logs
  fi

  if $JSON_OUTPUT; then
    print_json
  else
    print_table
  fi
}

main
