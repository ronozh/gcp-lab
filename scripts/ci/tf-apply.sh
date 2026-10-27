#!/usr/bin/env bash
# Plan + apply terraform/ in CI.
# Public repo => public logs: never print the plan or variable values.
# Prints only change counts and resource types; on error, only Terraform's error block.
# Usage: scripts/ci/tf-apply.sh   (expects terraform/backend.hcl and terraform/terraform.tfvars)
set -euo pipefail

dir=terraform
log=$(mktemp)

show_errors() {
  echo "::error::terraform failed"
  grep -E -A12 '^(│ )?Error' "$log" | head -60 || tail -20 "$log"
}

echo "== init"
terraform -chdir="$dir" init -input=false -no-color -backend-config=backend.hcl >"$log" 2>&1 || { show_errors; exit 1; }

echo "== plan"
set +e
terraform -chdir="$dir" plan -input=false -no-color -detailed-exitcode -out=tfplan >"$log" 2>&1
rc=$?
set -e
[ $rc -eq 1 ] && { show_errors; exit 1; }

# Counts per action + resource types only (addresses can contain values, e.g. emails).
summary=$(terraform -chdir="$dir" show -json tfplan | jq -r '
  [.resource_changes[]? | select(.change.actions != ["no-op"])] as $c
  | if ($c | length) == 0 then "No changes."
    else ($c | group_by(.change.actions | join("/"))
          | map("\(.[0].change.actions | join("/")): \(length) (\([.[].type] | unique | join(", ")))")
          | join("\n"))
    end')
echo "$summary"
{ echo "### terraform"; echo '```'; echo "$summary"; echo '```'; } >>"${GITHUB_STEP_SUMMARY:-/dev/null}"

if [ $rc -eq 2 ]; then
  echo "== apply"
  terraform -chdir="$dir" apply -input=false -no-color tfplan >"$log" 2>&1 || { show_errors; exit 1; }
  grep -E '^Apply complete' "$log"
fi
rm -f "$dir/tfplan" "$log"
