#!/usr/bin/env bash
# ============================================================================
# Grafana Cloud — FinOps Dashboard Provisioning Script
# ============================================================================
# Deploys:
#   1. Snowflake datasource (grafana-snowflake-datasource plugin)
#   2. FinOps dashboard (6 panels: credits, budget, warehouses, queries, storage)
#
# Prerequisites:
#   - Grafana Cloud instance with Snowflake plugin installed
#     (Connections > Add new connection > Snowflake > Install)
#   - Service Account Token with Admin role
#     (Administration > Service accounts > Add service account > Add token)
#
# Usage:
#   export GRAFANA_URL="https://stefentaime.grafana.net"
#   export GRAFANA_TOKEN="glsa_xxxx..."
#   export SNOWFLAKE_PASSWORD="your_password"
#   ./deploy.sh
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Validate env vars ────────────────────────────────────────────
: "${GRAFANA_URL:?Set GRAFANA_URL (e.g. https://stefentaime.grafana.net)}"
: "${GRAFANA_TOKEN:?Set GRAFANA_TOKEN (Service Account token with Admin role)}"
: "${SNOWFLAKE_PASSWORD:?Set SNOWFLAKE_PASSWORD}"

API="${GRAFANA_URL}/api"
AUTH="Authorization: Bearer ${GRAFANA_TOKEN}"
CT="Content-Type: application/json"

echo "==> Grafana Cloud: ${GRAFANA_URL}"

# ── Step 1: Check plugin is installed ────────────────────────────
echo ""
echo "--- Step 1: Checking Snowflake plugin ---"
PLUGIN_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "${AUTH}" \
  "${API}/plugins/grafana-snowflake-datasource/settings")

if [ "${PLUGIN_STATUS}" = "200" ]; then
  echo "OK: Snowflake plugin is installed."
else
  echo "WARN: Snowflake plugin not found (HTTP ${PLUGIN_STATUS})."
  echo "      Install it manually: Connections > Add new connection > Snowflake > Install"
  echo "      Then re-run this script."
  exit 1
fi

# ── Step 2: Create or update datasource ──────────────────────────
echo ""
echo "--- Step 2: Creating Snowflake datasource ---"

# Replace password placeholder in datasource JSON
DS_PAYLOAD=$(cat "${SCRIPT_DIR}/datasource.json" \
  | sed "s/__SNOWFLAKE_PASSWORD__/${SNOWFLAKE_PASSWORD}/g")

# Check if datasource already exists
EXISTING_DS=$(curl -s -H "${AUTH}" "${API}/datasources/name/Snowflake%20-%20FinOps" \
  -o /dev/null -w "%{http_code}")

if [ "${EXISTING_DS}" = "200" ]; then
  # Update existing
  DS_ID=$(curl -s -H "${AUTH}" "${API}/datasources/name/Snowflake%20-%20FinOps" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  RESULT=$(curl -s -w "\n%{http_code}" \
    -X PUT \
    -H "${AUTH}" -H "${CT}" \
    -d "${DS_PAYLOAD}" \
    "${API}/datasources/${DS_ID}")
  echo "Updated existing datasource (id: ${DS_ID})"
else
  # Create new
  RESULT=$(curl -s -w "\n%{http_code}" \
    -X POST \
    -H "${AUTH}" -H "${CT}" \
    -d "${DS_PAYLOAD}" \
    "${API}/datasources")
  HTTP_CODE=$(echo "${RESULT}" | tail -1)
  if [ "${HTTP_CODE}" = "200" ] || [ "${HTTP_CODE}" = "409" ]; then
    echo "OK: Datasource created."
  else
    echo "FAIL: Could not create datasource (HTTP ${HTTP_CODE})"
    echo "${RESULT}" | head -1
    exit 1
  fi
fi

# ── Step 3: Get datasource UID ───────────────────────────────────
echo ""
echo "--- Step 3: Resolving datasource UID ---"
DS_UID=$(curl -s -H "${AUTH}" "${API}/datasources/name/Snowflake%20-%20FinOps" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['uid'])")
echo "Datasource UID: ${DS_UID}"

# ── Step 4: Deploy dashboard ────────────────────────────────────
echo ""
echo "--- Step 4: Deploying FinOps dashboard ---"

# Read dashboard JSON and replace datasource UID placeholder
DASH_PAYLOAD=$(cat "${SCRIPT_DIR}/finops-dashboard.json" \
  | python3 -c "
import sys, json
data = json.load(sys.stdin)
# Replace datasource references in all panels
uid = '${DS_UID}'
for panel in data.get('dashboard', {}).get('panels', []):
    if 'datasource' in panel:
        panel['datasource']['uid'] = uid
    for target in panel.get('targets', []):
        if 'datasource' in target:
            target['datasource']['uid'] = uid
# Remove inputs (not needed for API import)
data.pop('inputs', None)
json.dump(data, sys.stdout)
")

DASH_RESULT=$(curl -s -w "\n%{http_code}" \
  -X POST \
  -H "${AUTH}" -H "${CT}" \
  -d "${DASH_PAYLOAD}" \
  "${API}/dashboards/db")

DASH_HTTP=$(echo "${DASH_RESULT}" | tail -1)
DASH_BODY=$(echo "${DASH_RESULT}" | head -1)

if [ "${DASH_HTTP}" = "200" ]; then
  DASH_URL=$(echo "${DASH_BODY}" | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || echo "")
  echo "OK: Dashboard deployed."
  echo "URL: ${GRAFANA_URL}${DASH_URL}"
else
  echo "FAIL: Could not deploy dashboard (HTTP ${DASH_HTTP})"
  echo "${DASH_BODY}"
  exit 1
fi

echo ""
echo "=== Deployment complete ==="
echo "Dashboard: ${GRAFANA_URL}/d/snowflake-finops/snowflake-finops-credit-cost-monitoring"
