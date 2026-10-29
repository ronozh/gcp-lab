#!/usr/bin/env bash
# The simulated ANZ side on AWS (aws/terraform), run locally with SSO.
#   scripts/aws.sh apply | destroy | plan
#   scripts/aws.sh upload <feed> [day]    sample file(s) -> s3://raw-<acct>/ecommerce/<feed>/...  (no day = all days)
#   scripts/aws.sh ls                     list raw + processed
# Login first: aws sso login --profile dpivoted (and gcloud auth application-default login for the bridge)
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_PROFILE=${AWS_PROFILE:-dpivoted}
TF_VAR_project_id=$(sed -n 's/^project_id *= *"\(.*\)"/\1/p' terraform/terraform.tfvars); export TF_VAR_project_id
tf() { terraform -chdir=aws/terraform "$@"; }
acct() { aws sts get-caller-identity --query Account --output text; }

case "${1:-}" in
  apply|destroy|plan)
    tf init -input=false -backend-config=../../terraform/backend.hcl >/dev/null
    tf "$@" ;;
  upload)
    feed=${2:?feed}; src=data/sample/ecommerce/$feed
    if [ -n "${3:-}" ]; then src=$src/$3; dst=ecommerce/$feed/$3; else dst=ecommerce/$feed; fi
    aws s3 cp --recursive --exclude "*" --include "*.csv" "$src" "s3://raw-$(acct)/$dst" ;;
  ls)
    a=$(acct)
    for b in raw processed; do echo "== $b"; aws s3 ls --recursive "s3://$b-$a/" | awk '{print $3, $4}'; done ;;
  *) sed -n 2,7p "$0"; exit 1 ;;
esac
