# 00 · Bootstrap: GitHub → GCP without keys

**What this phase does:** a one-time setup you run as owner. Afterwards, GitHub Actions manages all infrastructure with Terraform.

## Mental model: GCP vs AWS

| GCP | AWS ≈ | Note |
|---|---|---|
| **Billing account** | Payer account | Pays for projects. Free-trial credit lives here |
| **Project** | Account | Unit of isolation, IAM and billing. Has an ID (you choose; globally unique) and a number (Google assigns) |
| **API / service** (`bigquery.googleapis.com`) | — | Every service is **off** in a new project. `gcloud services enable` turns it on |
| **IAM binding** = principal + role, set **on a resource** | Resource-based policy (like a bucket policy) | Exists on every resource; inherited org → folder → project → resource |
| **Role** (`roles/storage.admin`) | Managed policy | A bundle of permissions |
| **Service account (SA)** | IAM role | Identity for machines. Who may *act as it* (`workloadIdentityUser`, `serviceAccountUser`) ≈ the role's trust policy + `sts:AssumeRole` |
| **Workload Identity Federation (WIF)** | OIDC provider + `AssumeRoleWithWebIdentity` | Two calls: `sts.googleapis.com` exchanges the GitHub token, then `iamcredentials` issues the SA token |
| **GCS bucket** (`gs://…`) | S3 bucket | Object storage |

## How CI logs in (no keys)

```
GitHub job (env dev, main) ──OIDC token──▶ STS: pool "github" / provider "github"
     checks condition: repository_id, repository_owner_id, ref == main, environment == dev
                                            ──▶ federated identity
             roles/iam.workloadIdentityUser on the SA (for this repo id)
                                            ──▶ act as gcp-lab-ci@… (1-hour token) ──▶ Terraform
```
- **Two guards:**
  - GitHub's `dev` environment only deploys from `main`, so a job on another branch is rejected before it even starts.
  - The WIF condition is the cloud-side backstop.
- **Numeric IDs, not names:** repo and owner names can be renamed or re-registered by someone else. The IDs never change.
- No JSON key exists to leak.

## Run it (once)

```bash
gcloud auth login
gcloud projects create <project-id> --name=gcp-lab
gcloud billing projects link <project-id> --billing-account=<billing-account>
cp bootstrap/.env.example bootstrap/.env    # fill in; repo/owner ids: gh api repos/OWNER/REPO --jq '.id, .owner.id'
bootstrap/bootstrap.sh                      # GCP side (idempotent)
scripts/github-setup.sh                     # GitHub side: hardening, env `dev`, secrets
```

## What the scripts create

| Thing | Name | Why |
|---|---|---|
| APIs | iam, sts, iamcredentials, storage, serviceusage, cloudresourcemanager, billingbudgets | Needed by WIF and Terraform |
| Bucket | `gs://<project-id>-tfstate` (versioned) | Terraform state (≈ S3 backend). Locking is built in |
| SA | `gcp-lab-ci@…` | The identity CI runs as. Roles: storage, BigQuery, IAM, network admin, … (near-owner; fine for a lab) |
| WIF | pool `github`, provider `github` | Trusts GitHub's OIDC tokens (condition above) |
| Budget | `gcp-lab`, 30 AUD, 50/90/100% | Emails billing account admins/users. Counts cost **before** trial credits, so alerts are real |
| GitHub | env `dev` (main only), 5 secrets, actions allowlist + SHA pins | Public repo safety |

## Verify it yourself

```bash
source bootstrap/.env
gcloud services list --enabled --project=$PROJECT_ID
gcloud storage buckets describe gs://$PROJECT_ID-tfstate --format="value(versioning_enabled)"     # True
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten=bindings --filter="bindings.members:gcp-lab-ci" --format="value(bindings.role)"        # CI roles
gcloud iam workload-identity-pools providers describe github --project=$PROJECT_ID \
  --workload-identity-pool=github --location=global --format="value(attributeCondition)"          # the trust rule
gcloud iam service-accounts keys list --managed-by=user \
  --iam-account=gcp-lab-ci@$PROJECT_ID.iam.gserviceaccount.com                                    # empty = no keys
gcloud billing budgets list --billing-account=$BILLING_ACCOUNT --billing-project=$PROJECT_ID
```

## Try it

```bash
gh workflow run terraform && sleep 5 && gh run watch "$(gh run list -w terraform -L1 --json databaseId -q '.[0].databaseId')"
```
Expected: `validate` ✓, then `dev` logs in via WIF, prints a plan summary and goes green.

## Gotchas
- A project ID is globally unique across all customers, so use something like `gcp-lab-<suffix>`.
- IAM changes take **minutes** to propagate. The first CI run failed with `getAccessToken denied`; a rerun passed.
- The auth action exports the project ID as env vars that appear in log headers. The workflow masks it, along with the project number.
- Budgets include credits by default, so they would show ~0 during the trial. We use `exclude-all-credits`.
