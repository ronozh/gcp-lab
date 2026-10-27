# 04 · Governance: column + row security in BigQuery (≈ Lake Formation)

**What this phase does:** on `gold.fct_sales`, PII columns can be read only by a privileged identity, and one analyst sees only NSW rows. You test it by **impersonating** test identities.

## Mental model

```
                 gold.fct_sales
 columns:  customer_email ── policy tag pii/high ─┐   only "fine-grained readers" of the tag
           customer_first/last_name ── pii/medium ┘   may SELECT these columns (others: Access Denied)
 rows:     row access policy nsw_only → lab-analyst-nsw sees customer_state = 'NSW'
           row access policy all_rows → you, lab-admin, lab-analyst see everything
```

| GCP | Lake Formation ≈ | Note |
|---|---|---|
| **Taxonomy** `pii` + **policy tags** `high`, `medium` | LF-tag `pii` with values | Created by Terraform; attached to columns by dbt |
| **Fine-grained reader** role on a tag | LF-tag grant (SELECT on tagged columns) | Without it, selecting the column fails, **even for the project owner** |
| **Row access policy** (`FILTER USING …`, `GRANT TO …`) | Data filter (row filter) | Once a table has *any* policy, a user in **no** policy sees **0 rows** |
| Data masking policy (hash / NULL) | Cell-level masking | **Not used:** needs a GCP **Organization**, which a personal gmail project has none of |
| IAM on dataset (`bigquery.dataViewer` on `gold`) | Table/database grant | Test identities can read `gold` only, not `silver` or buckets |

## Who gets what (verified)

| Identity | PII columns | Rows | silver |
|---|---|---|---|
| `lab-admin` | ✅ values | all 11,614 | denied |
| `lab-analyst` | ❌ Access Denied | all 11,614 | denied |
| `lab-analyst-nsw` | ❌ Access Denied | 1,322 (NSW) | denied |
| you (project owner) | ❌ Access Denied | all 11,614 | (owner) |

Analysts query PII-free columns normally: `SELECT * EXCEPT (customer_email, customer_first_name, customer_last_name) FROM gold.fct_sales`.

## Where each piece lives

| Piece | File |
|---|---|
| Taxonomy, policy tags, test SAs, IAM (tag reader, dataset viewer, job user, you may impersonate them) | `terraform/governance.tf` |
| Tags on columns | `dbt/models/gold/gold.yml` (`policy_tags`) + `+persist_docs: {columns: true}` |
| Row access policies | `dbt/dbt_project.yml` gold `+post-hook` |
| Tag IDs (contain the project id, so not in git) | looked up by `scripts/run-dbt.sh` → `PII_HIGH_TAG`, `PII_MEDIUM_TAG` |

## Commands

```bash
scripts/run-dbt.sh build                 # (re)applies tags + row policies
scripts/test-governance.sh               # the table above, live
```

Impersonate manually (≈ `aws sts assume-role`):
```bash
source bootstrap/.env
SA=lab-analyst-nsw@$PROJECT_ID.iam.gserviceaccount.com
CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=$SA bq query --nouse_legacy_sql \
  'SELECT customer_state, COUNT(*) FROM gold.fct_sales GROUP BY 1'
```

## Verify it yourself

```bash
bq show --format=prettyjson gold.fct_sales | jq -r '.schema.fields[] | select(.policyTags) | .name'   # tagged columns
bq ls --row_access_policies gold.fct_sales                                                            # policies + grantees
gcloud data-catalog taxonomies list --location=$REGION                                                # the taxonomy
```
Console: BigQuery → `gold.fct_sales` → **Schema** (policy tag icon) · BigQuery → **Policy tags** (taxonomy, who can read).

## Try it
- **Grant yourself PII:** give your user `roles/datacatalog.categoryFineGrainedReader` on tag `high` (Console → Policy tags → `high` → Add principal). Your email query then works; names still fail (`medium`).
- **Remove a grantee:** take `lab-analyst` out of `all_rows` in `dbt_project.yml`, then build and test. It now sees **0 rows**, not an error.

## Gotchas
- **Rebuild window:** each dbt build is `CREATE OR REPLACE`, so for a few seconds the table has no tags or policies, until `persist_docs` and the post-hooks re-apply them. That's fine for a lab, not for production (use incremental models or swap tables).
- **BigQuery-only:** the Iceberg Parquet files in `gs://<project-id>-lake/iceberg/gold/` hold clear PII. Anyone with bucket access bypasses all of this, just as Lake Formation doesn't protect raw S3. The test identities have no bucket access.
- **No masking:** data policies (`SHA256`, `ALWAYS_NULL`) need an Organization. So we deny the column instead of masking it.
- **dbt tests:** no tests on tagged columns, because you, the dbt runner, have no fine-grained reader role.
