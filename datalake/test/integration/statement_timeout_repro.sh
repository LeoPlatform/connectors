#!/usr/bin/env bash
# STATEMENT_TIMEOUT repro — uses `databricks api` for auth/transport.
#
# Expected: query with session_parameters.STATEMENT_TIMEOUT=5 is CANCELED
#           within ~5s of execution starting.
# Observed: query runs to SUCCEEDED (~70s); STATEMENT_TIMEOUT is ignored.
#
# Usage:
#   ./statement_timeout_repro.sh
#   DATABRICKS_CONFIG_PROFILE=my-profile WAREHOUSE_ID=abc123 ./statement_timeout_repro.sh

set -euo pipefail

PROFILE="${DATABRICKS_CONFIG_PROFILE:-dev-cup}"
WAREHOUSE_ID="${WAREHOUSE_ID:-5d84579f11466e3f}"
DBC="databricks --profile $PROFILE"

SLOW_SQL="SELECT max(running_sum) FROM (SELECT id, sum(id) OVER (ORDER BY id) AS running_sum FROM range(500000000)) t"

echo "STATEMENT_TIMEOUT repro"
echo "  profile:      $PROFILE"
echo "  warehouse:    $WAREHOUSE_ID"
echo

# Step 1: warm the warehouse so the timeout applies to execution, not cold-start.
echo "Step 1: warming warehouse..."
$DBC api post /api/2.0/sql/statements --json "{
  \"warehouse_id\": \"$WAREHOUSE_ID\",
  \"statement\": \"SELECT 1\",
  \"wait_timeout\": \"50s\",
  \"disposition\": \"INLINE\"
}" | python3 -c "import sys,json; r=json.load(sys.stdin); print(f'  state={r[\"status\"][\"state\"]}')"
echo

# Step 2: submit the slow query with STATEMENT_TIMEOUT=5.
echo "Step 2: submitting slow query with STATEMENT_TIMEOUT=5..."
STMT_RESP=$($DBC api post /api/2.0/sql/statements --json "{
  \"warehouse_id\": \"$WAREHOUSE_ID\",
  \"statement\": \"$SLOW_SQL\",
  \"wait_timeout\": \"5s\",
  \"disposition\": \"INLINE\",
  \"session_parameters\": {\"STATEMENT_TIMEOUT\": \"5\"}
}")
STMT_ID=$(echo "$STMT_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['statement_id'])")
STATE=$(echo "$STMT_RESP"  | python3 -c "import sys,json; print(json.load(sys.stdin)['status']['state'])")
echo "  statement_id = $STMT_ID"
echo "  state after 5s = $STATE"
echo

# Step 3: poll for 30s — STATEMENT_TIMEOUT=5 should cancel it well before then.
echo "Step 3: polling — expect CANCELED within ~5s..."
printf "  %6s  %s\n" "t(s)" "state"
printf "  %6s  %s\n" "------" "-----"
T_START=$(date +%s)
FINAL_STATE=""
for i in $(seq 1 30); do
    sleep 1
    POLL=$($DBC api get "/api/2.0/sql/statements/$STMT_ID")
    STATE=$(echo "$POLL" | python3 -c "import sys,json; print(json.load(sys.stdin)['status']['state'])")
    ELAPSED=$(( $(date +%s) - T_START ))
    ERROR=$(echo "$POLL" | python3 -c "
import sys,json
r=json.load(sys.stdin)
e=r['status'].get('error',{})
print(f'  error_code={e[\"error_code\"]}' if e else '')
" 2>/dev/null || true)
    printf "  %6d  %s%s\n" "$ELAPSED" "$STATE" "$ERROR"
    if [[ "$STATE" != "PENDING" && "$STATE" != "RUNNING" ]]; then
        FINAL_STATE="$STATE"
        break
    fi
done

echo
# Step 4: verdict.
if [[ "$FINAL_STATE" == "CANCELED" || "$FINAL_STATE" == "FAILED" ]]; then
    ELAPSED=$(( $(date +%s) - T_START ))
    if (( ELAPSED < 15 )); then
        echo "PASS: STATEMENT_TIMEOUT fired — query ended as $FINAL_STATE in ${ELAPSED}s"
    else
        echo "FAIL: query ended as $FINAL_STATE but after ${ELAPSED}s (expected <15s)"
    fi
elif [[ -n "$FINAL_STATE" ]]; then
    echo "FAIL: query ended as $FINAL_STATE — not due to STATEMENT_TIMEOUT"
else
    echo "FAIL: query still RUNNING after 30s — STATEMENT_TIMEOUT did NOT fire"
    echo "      Cancelling..."
    $DBC api post "/api/2.0/sql/statements/$STMT_ID/cancel" --json '{}' > /dev/null && echo "      Cancelled."
    echo
    echo "Expected : state=CANCELED within ~5s of execution start"
    echo "Observed : RUNNING indefinitely despite session_parameters.STATEMENT_TIMEOUT=5"
    echo
    echo "Environment:"
    echo "  Warehouse type : SQL Serverless (Small)"
    echo "  Parameter set  : session_parameters.STATEMENT_TIMEOUT = \"5\" (seconds)"
    echo "  Query          : window sum over range(500000000) — takes ~70s without timeout"
fi
