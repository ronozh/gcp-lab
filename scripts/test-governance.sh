#!/usr/bin/env bash
# Run the same queries as each identity and show what each one gets (column + row security on gold.fct_sales).
# Usage: scripts/test-governance.sh
set -uo pipefail   # no -e: "Access Denied" is an expected result here
root=$(cd "$(dirname "$0")/.." && pwd)
source "$root/bootstrap/.env"

pii="SELECT customer_email, customer_first_name FROM gold.fct_sales LIMIT 1"
rows="SELECT COUNT(*) AS n, STRING_AGG(DISTINCT customer_state ORDER BY customer_state) AS states FROM gold.fct_sales"
silver="SELECT COUNT(*) AS n FROM silver.customer"

run() {   # $1 = identity to impersonate ("" = you), $2 = label, $3 = SQL
  out=$(CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=$1 bq query --nouse_legacy_sql --location="$REGION" \
        --format=csv "$3" 2>&1 | grep -vE 'WARNING|impersonat|^$')
  if grep -q 'Access Denied' <<<"$out"; then echo "  $2: ACCESS DENIED"
  else echo "  $2: $(tail -n +2 <<<"$out" | tr '\n' ' ')"; fi
}

for who in lab-admin lab-analyst lab-analyst-nsw you; do
  sa=""; [ "$who" = you ] || sa="$who@$PROJECT_ID.iam.gserviceaccount.com"
  echo "== $who"
  run "$sa" "PII columns " "$pii"
  run "$sa" "rows        " "$rows"
  [ "$who" = you ] || run "$sa" "silver      " "$silver"
done
