# 00 · Bootstrap: GitHub → GCP without keys

**What this phase does:** a one-time setup you run as owner. Afterwards, GitHub Actions manages all infrastructure with Terraform.

## Mental model: GCP vs AWS

| GCP | AWS ≈ | Note |
|---|---|---|
| **Billing account** | Payer account | Pays for projects. Free-trial credit lives here |
| **Project** (`<project-id>`) | Account | Unit of isolation, IAM and billing. Has an ID (yours, global) and a number (Google's) |
| **API / service** (`bigquery.googleapis.com`) | — | Every service is **off** in a new project. `gcloud services enable` turns it on |
| **IAM binding** = principal + role (+ resource) | Policy attached to a role/user | GCP attaches *who can do what* **to the resource** (project, bucket, SA) |
| **Role** (`roles/storage.admin`) | Managed policy | A bundle of permissions |
| **Service account (SA)** | IAM role | Identity for machines. Others are *allowed to act as it* |
| **Workload Identity Federation (WIF)** | OIDC provider + `AssumeRoleWithWebIdentity` | Trust external tokens (GitHub) → short-lived SA credentials |
| **GCS bucket** (`gs://…`) | S3 bucket | Object storage |

## How CI logs in (no keys)

```
GitHub job ──OIDC token (repo_id, ref, …)──▶ STS: pool "github" / provider "github"
                                              checks condition: repo id + owner id + ref == main
                                              ──▶ federated identity
                       roles/iam.workloadIdentityUser (on the SA)
                                              ──▶ acts as gcp-lab-ci@… (1-hour token) ──▶ Terraform
```
- **Provider condition** = who may log in at all: only this repo, on `main`.
- **workloadIdentityUser binding** = which SA they may act as.
- No JSON key exists to leak.

## What was created

| Thing | Name | Why |
|---|---|---|
| Project | `<project-id>` (linked to billing) | Everything lives here; delete the project = delete everything |
| APIs | iam, sts, iamcredentials, storage, serviceusage, cloudresourcemanager, billingbudgets | Needed by WIF and Terraform |
| Bucket | `gs://<project-id>-tfstate` (versioned) | Terraform state (≈ S3 backend). Locking is built in |
| SA | `gcp-lab-ci@…` | The identity CI runs as. Roles: storage, BigQuery, IAM admin, … (near-owner; fine for a lab) |
| WIF | pool `github`, provider `github` | Trusts GitHub's OIDC tokens |
| Budget | `gcp-lab`, 30 AUD, 50/90/100% | Emails billing admins. Counts cost **before** trial credits, so alerts are real |
| GitHub | env `dev` (main only), 5 secrets, actions allowlist + SHA pins | Public repo safety |

## Files

| File | Does |
|---|---|
| `bootstrap/bootstrap.sh` | GCP side (idempotent, rerun any time). Config in `bootstrap/.env` (gitignored) |
| `scripts/github-setup.sh` | GitHub side: hardening, `dev` environment, secrets |
| `.github/workflows/terraform.yml` | validate → `dev` job: WIF login → `scripts/ci/tf-apply.sh` |
| `terraform/` | Phase 0: only reads the project (proves login works) |

## Verify it yourself

```bash
P=$(gcloud config get project)                              # your project id
gcloud services list --enabled                              # enabled APIs
gcloud storage buckets describe gs://$P-tfstate --format="value(versioning_enabled)"   # True
gcloud projects get-iam-policy $P \
  --flatten=bindings --filter="bindings.members:gcp-lab-ci" --format="value(bindings.role)"        # CI roles
gcloud iam workload-identity-pools providers describe github \
  --workload-identity-pool=github --location=global --format="value(attributeCondition)"           # the trust rule
gcloud iam service-accounts keys list --iam-account=gcp-lab-ci@$P.iam.gserviceaccount.com \
  --managed-by=user                                                                                # empty = no keys
gcloud billing budgets list --billing-account=<billing-account> --billing-project=$P
```

## Try it

```bash
gh workflow run terraform && gh run watch      # CI logs in via WIF; plan shows "No changes."
```
- **To see the trust work:** run the workflow from another branch. It fails at `auth`, because the condition requires `main`.

## Gotchas
- A project ID is globally unique across all customers, hence an ID like `gcp-lab-<suffix>`, not `gcp-lab`.
- A new SA can take seconds to appear in IAM; the script retries role bindings.
- Budgets include credits by default, so they would show ~0 during the trial. We use `exclude-all-credits`.
