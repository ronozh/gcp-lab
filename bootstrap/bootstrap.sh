#!/usr/bin/env bash
# One-time GCP setup, run by the project owner. Idempotent: safe to re-run.
# Creates what Terraform itself needs: APIs, a state bucket, the CI identity (WIF), and a budget.
# Usage: bootstrap/bootstrap.sh   (reads bootstrap/.env)
set -euo pipefail
cd "$(dirname "$0")"
source .env

export CLOUDSDK_CORE_PROJECT="$PROJECT_ID" # this script only; your gcloud config is untouched
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
STATE_BUCKET="gs://${PROJECT_ID}-tfstate"
CI_SA="gcp-lab-ci@${PROJECT_ID}.iam.gserviceaccount.com"
POOL=github
PROVIDER=github

echo "1. APIs"
gcloud services enable \
  serviceusage.googleapis.com cloudresourcemanager.googleapis.com iam.googleapis.com \
  iamcredentials.googleapis.com sts.googleapis.com storage.googleapis.com \
  billingbudgets.googleapis.com compute.googleapis.com

echo "1b. Private Google Access on the default subnet (Spark workers have no public IP)"
gcloud compute networks subnets update default --region="$REGION" --enable-private-ip-google-access

echo "2. Terraform state bucket: $STATE_BUCKET"
gcloud storage buckets describe "$STATE_BUCKET" >/dev/null 2>&1 ||
  gcloud storage buckets create "$STATE_BUCKET" --location="$REGION" \
    --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update "$STATE_BUCKET" --versioning >/dev/null

echo "3. CI service account + roles: $CI_SA"
gcloud iam service-accounts describe "$CI_SA" >/dev/null 2>&1 ||
  gcloud iam service-accounts create gcp-lab-ci --display-name="GitHub Actions CI (Terraform)"
for role in \
  roles/storage.admin roles/bigquery.admin roles/iam.serviceAccountAdmin \
  roles/resourcemanager.projectIamAdmin roles/serviceusage.serviceUsageAdmin \
  roles/datacatalog.categoryAdmin roles/bigquerydatapolicy.admin; do
  # A new SA can take a few seconds to become visible to IAM: retry, then fail loudly.
  for i in 1 2 3 4 5; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$CI_SA" \
      --role="$role" --condition=None --quiet >/dev/null && break
    [ "$i" = 5 ] && { echo "binding $role failed" >&2; exit 1; }
    sleep 5
  done
done

echo "4. Workload Identity Federation: GitHub OIDC -> $CI_SA"
gcloud iam workload-identity-pools describe "$POOL" --location=global >/dev/null 2>&1 ||
  gcloud iam workload-identity-pools create "$POOL" --location=global --display-name="GitHub Actions"
# Numeric IDs, not names: names can be renamed or re-registered by someone else.
oidc_args=(
  --workload-identity-pool="$POOL" --location=global
  --issuer-uri="https://token.actions.githubusercontent.com"
  --attribute-mapping="google.subject=assertion.sub,attribute.repository_id=assertion.repository_id,attribute.ref=assertion.ref"
  --attribute-condition="assertion.repository_id=='${GITHUB_REPO_ID}' && assertion.repository_owner_id=='${GITHUB_OWNER_ID}' && assertion.ref=='refs/heads/main' && assertion.environment=='dev'"
)
if gcloud iam workload-identity-pools providers describe "$PROVIDER" --workload-identity-pool="$POOL" --location=global >/dev/null 2>&1; then
  gcloud iam workload-identity-pools providers update-oidc "$PROVIDER" "${oidc_args[@]}" >/dev/null
else
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" "${oidc_args[@]}" --display-name="GitHub"
fi
gcloud iam service-accounts add-iam-policy-binding "$CI_SA" --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/attribute.repository_id/${GITHUB_REPO_ID}" \
  >/dev/null

echo "5. Budget: 30 AUD, alerts at 50/90/100% of cost before free-trial credits"
# Capture first so a failed list stops the script (instead of creating a duplicate budget).
budgets=$(gcloud billing budgets list --billing-account="$BILLING_ACCOUNT" --billing-project="$PROJECT_ID" \
  --format='value(displayName)')
if ! grep -qx gcp-lab <<<"$budgets"; then
  gcloud billing budgets create --billing-account="$BILLING_ACCOUNT" --billing-project="$PROJECT_ID" \
    --display-name=gcp-lab --budget-amount=30AUD \
    --filter-projects="projects/${PROJECT_NUMBER}" --credit-types-treatment=exclude-all-credits \
    --threshold-rule=percent=0.5 --threshold-rule=percent=0.9 --threshold-rule=percent=1.0
fi

echo "Done. Next: scripts/github-setup.sh"
