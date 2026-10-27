# gcp-lab

A medallion lakehouse on GCP, built from code. Sibling of `../snowflake-lab` (same 4 feeds). Public repo `ronozh/gcp-lab`.

## Priorities
1. A working end-to-end solution. 2. Kept very simple. 3. Show a broad tech stack, each piece once.

## Rules
- Responses: very concise.
- Work through `plan/README.md` one phase at a time: build → verify against done-when → update `docs/` → independent code + doc review (agent) → fix agreed issues → commit.
- The user is **new to GCP** (strong on AWS/Snowflake). `docs/` is a top deliverable: for every step, a very clear, concise doc building a mental model (GCP concept ≈ AWS equivalent), with the commands and how to verify and try it. Update it with every change.
- Public repo: no secrets, keys, state, tfvars, or project/billing IDs in git.
- Log each error and its fix in `gotcha.md` (gitignored).

## Stack
GCS landing → Serverless Spark (PySpark) → bronze Iceberg → dbt + BigQuery → silver views → gold Iceberg. Terraform applied by GitHub Actions (Workload Identity Federation, no keys). Region `australia-southeast1`.

## Layers
landing = GCS files (versioned) · bronze = Iceberg, typed with provenance, 1:1 per file, append-only, incremental by file name · silver = views over the latest file · gold = business tables.
