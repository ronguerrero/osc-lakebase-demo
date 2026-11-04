# OSC Lakebase Demos

A Databricks App with hands-on Lakebase Autoscaling demos, built on an Ontario Securities Commission registrant-oversight dataset (advisors, investors, accounts, holdings, KYC profiles, contributions, complaints, restitution payments; ~570K rows across 10 tables).

Everything is deployed with a Declarative Automation Bundle (DAB). You can deploy it from the Databricks workspace UI, so nothing needs to be installed on your machine.

## Demos

| Demo | What it shows |
|------|---------------|
| **Lakebase as Code (DABs)** | Deploy your own Lakebase project, or a project plus an app bound to it, from a bundle. Pick names, review the YAML, watch the deploy, then destroy it |
| **Lakehouse vs Lakebase** | Browse and edit the same data through the DBSQL Statement API, DBSQL Connector, Lakebase Postgres and the Lakebase Data API; OLTP vs OLAP benchmark |
| **Branching Workflows** | Create an isolated dev branch from production, make bulk changes, compare side by side, reset |
| **Reverse ETL** | Sync a Delta table into Lakebase Postgres, add rows, watch them arrive |
| **Scale-to-Zero** | Set inactivity timeouts, observe suspend and wake-up, compare cold vs warm latency |
| **Row-Level Security** | Switch between district-office personas; Postgres RLS filters the same query differently |
| **Point-in-Time Restore** | Simulate a bad update, then recover by branching from a past timestamp |
| **CI/CD Schema Migration** | Promote changelog-based migrations through dev, staging and production branches, with rollback |
| **Autoscaling Under Load** | Fire waves of concurrent queries and watch latency improve as compute scales up |
| **ORM (SQLAlchemy)** | Reflect tables, browse with filters, follow foreign keys, compare ORM vs raw SQL |

## Requirements

- A Databricks workspace with Unity Catalog, serverless compute, Databricks Apps and Lakebase Autoscaling
- Permission to create apps, Lakebase projects and jobs, and to create a schema in your chosen catalog
- A SQL warehouse, or permission to create one (the setup job creates a serverless 2X-Small warehouse if none has the configured name)

## Deploy from the workspace (no local tools)

1. **Add the repo**: *Workspace > Create > Git folder*, and paste this repo's URL.
2. **Configure**: open `databricks.yml` in the Git folder and edit the target you will use (see [Configuration](#configuration)). For your own workspace, set `catalog` under the `sandbox` target; for Databricks Free Edition use the `free` target as is.
3. **Deploy**: with `databricks.yml` open, open the bundle panel (*Deployments* icon on the left of the editor), choose the target and click **Deploy**. This creates the app and the setup job.
4. **Run setup**: in the same panel, run **OSC Lakebase Demo - Setup** (or find it under *Jobs & Pipelines*). It takes about 10-15 minutes and:
   - creates the schema, SQL warehouse (if needed) and the 10 Delta tables
   - creates the Lakebase project, database and Postgres tables with the same data
   - grants the app's service principal what it needs (warehouse, schema, Lakebase project, Postgres roles and row-level security)
   - deploys and starts the app
5. **Open the app** from *Compute > Apps*, or from the link at the end of the setup job's last task.

Re-running the setup job is safe: it skips anything that already exists. After editing code, click **Deploy** again; the app picks up the change on its next deployment (run the setup job, or deploy the app from *Compute > Apps*).

## Deploy from a terminal (optional)

With the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) (Windows: `winget install Databricks.DatabricksCLI`):

```bash
databricks auth login https://<your-workspace-url> --profile sandbox
databricks bundle deploy -t sandbox -p sandbox
databricks bundle run setup_osc_demo -t sandbox -p sandbox
```

Any variable can be overridden per deploy, e.g. `--var catalog=my_catalog --var app_name=osc-lakebase-jane`.

## Configuration

All workspace-specific settings are bundle variables in `databricks.yml`:

| Variable | Default | Meaning |
|----------|---------|---------|
| `catalog` | *(set per target)* | Catalog for the Delta tables. It must exist, or you need CREATE CATALOG |
| `schema` | `osc_demo` | Schema created in that catalog |
| `warehouse_name` | `osc-lakebase-demo-wh` | SQL warehouse to use; created if no warehouse has this name |
| `lakebase_project` | `osc-lakebase-demo` | Lakebase project id |
| `lakebase_db` | `osc_demo` | Postgres database in the project |
| `app_name` | `osc-lakebase-demo` | App name (unique per workspace) |
| `olap_periods` | `50` | OLAP benchmark size in 100K-row periods (50 = 5M rows, ~1.5 GB in Lakebase) |
| `dab_max_deployments` | `5` | Max concurrent deployments from the Lakebase as Code tile |

If several people deploy into the **same** workspace, each one should set their own `app_name`, `lakebase_project` and `schema` (for example with a name suffix).

### Targets

- **`sandbox`** (default): your own workspace. It has no host, so it deploys to the workspace you are in, or to your CLI profile. Set `catalog`.
- **`free`**: fallback for [Databricks Free Edition](https://docs.databricks.com/getting-started/free-edition). Uses the `workspace` catalog and the *Serverless Starter Warehouse*, and sizes the demo for Free Edition limits.

### Free Edition limits

The `free` target already accounts for these; they explain why some demos behave differently there:

- **512 MB per Lakebase project**: the OLAP table is 1M rows (`olap_periods: 10`) instead of 5M
- **3 Lakebase projects per workspace**: the main demo uses one, so the Lakebase as Code tile allows 2 deployments at a time
- **About 4 Lakebase endpoints running at once**: reset one branching demo (Branching, CI/CD, Point-in-Time Restore) before starting another
- **Fixed compute size and suspend timeout**: the Scale-to-Zero and Autoscaling demos show the current settings, but can't change them

## Manual steps after setup

- **USE CATALOG for the app**: if the person who ran setup doesn't own the catalog, a catalog owner runs `GRANT USE CATALOG ON CATALOG <catalog> TO \`<app service principal id>\`` once (the id is on the app's *Authorization* tab). Not needed on Free Edition or when you own the catalog.
- **Lakebase Data API** (only for the Data API source in *Lakehouse vs Lakebase*):
  1. In the Lakebase project, open *Data API* for the `production` branch and database `osc_demo`, and click **Enable**.
  2. Under *Settings > Exposed schemas*, add `lakebase_demo` and save.
  3. Re-run the setup job, which grants the app's role to the Data API (`authenticator`), then click **Refresh schema cache**.

## How it fits together

- `databricks.yml`: bundle variables and targets
- `resources/osc_app.app.yml`: the app, with its settings passed in as environment variables
- `resources/setup_job.yml`: the setup job (three serverless notebook tasks)
- `src/notebooks/`: `01_setup_lakehouse` (warehouse, schema, Delta data), `02_setup_lakebase` (Lakebase project, database, Postgres data), `03_grant_app_access` (permissions, deploys the app)
- `app/`: FastAPI app (`main.py`), demo pages (`static/`), Lakebase as Code tile (`dab_demo.py`, templates in `dab_templates/`)

The app looks up the SQL warehouse id and the Lakebase endpoint host by name when it starts, so no IDs are hardcoded and redeploys need no extra steps.

## Cleanup

1. Destroy any deployments made from the *Lakebase as Code* tile (its **Destroy** buttons).
2. In the bundle panel click **Destroy** (or run `databricks bundle destroy -t <target>`). This removes the app and the setup job.
3. Delete what the setup job created:
   - the Lakebase project (*Lakebase* page, or `databricks postgres delete-project projects/<lakebase_project>`)
   - the schema: `DROP SCHEMA <catalog>.<schema> CASCADE`
   - the SQL warehouse, if setup created it

## Credits and disclaimer

Based on [Karthik Subbarao's Lakebase demos](https://github.com/Karthikeya108/lakebase_demos), adapted for OSC.

This sample is meant to help you get started and is not production-ready. It comes with no guarantees or support; use it at your own risk. Parts of it were generated with AI.
