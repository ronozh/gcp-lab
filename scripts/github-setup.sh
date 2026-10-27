#!/usr/bin/env bash
# GitHub side of the bootstrap: repo hardening, `dev` environment, CI secrets. Idempotent.
# Usage: scripts/github-setup.sh   (reads bootstrap/.env; needs `gh` logged in as the repo owner)
set -euo pipefail
cd "$(dirname "$0")/.."
source bootstrap/.env
repo=$GITHUB_REPO
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')

echo "secret scanning + push protection"
gh api -X PATCH "repos/$repo" --silent \
  -f 'security_and_analysis[secret_scanning][status]=enabled' \
  -f 'security_and_analysis[secret_scanning_push_protection][status]=enabled'

echo "actions: allowlist, SHA pinning, read-only token"
gh api -X PUT "repos/$repo/actions/permissions" --silent \
  -F enabled=true -f allowed_actions=selected -F sha_pinning_required=true
gh api -X PUT "repos/$repo/actions/permissions/selected-actions" --silent \
  -F github_owned_allowed=true -F verified_allowed=false \
  -f 'patterns_allowed[]=hashicorp/*' -f 'patterns_allowed[]=google-github-actions/*'
gh api -X PUT "repos/$repo/actions/permissions/workflow" --silent \
  -f default_workflow_permissions=read -F can_approve_pull_request_reviews=false
gh api -X PUT "repos/$repo/actions/permissions/fork-pr-contributor-approval" --silent \
  -f approval_policy=all_external_contributors

echo "environment dev: deploy from main only"
gh api -X PUT "repos/$repo/environments/dev" --silent --input - <<'JSON'
{"deployment_branch_policy": {"protected_branches": false, "custom_branch_policies": true}}
JSON
if ! gh api "repos/$repo/environments/dev/deployment-branch-policies" --jq '.branch_policies[].name' | grep -qx main; then
  gh api -X POST "repos/$repo/environments/dev/deployment-branch-policies" --silent -f name=main -f type=branch
fi

echo "dev secrets"
set_secret() { printf '%s' "$2" | gh secret set "$1" --env dev --repo "$repo"; }
set_secret GCP_PROJECT_ID   "$PROJECT_ID"
set_secret GCP_WIF_PROVIDER "projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github/providers/github"
set_secret GCP_CI_SA        "gcp-lab-ci@${PROJECT_ID}.iam.gserviceaccount.com"
set_secret TF_STATE_BUCKET  "${PROJECT_ID}-tfstate"
set_secret TFVARS "$(printf 'project_id  = "%s"\nowner_email = "%s"\n' "$PROJECT_ID" "$OWNER_EMAIL")"
