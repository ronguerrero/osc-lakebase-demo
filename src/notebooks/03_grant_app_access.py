# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Configure App - Grant SP Access & Set Environment
# MAGIC Discovers the app's service principal, creates a Lakebase OAuth role,
# MAGIC grants schema/table access, and configures the app with discovered resource IDs.
# MAGIC All operations use the executor's identity.

# COMMAND ----------

# MAGIC %md
# MAGIC Plain `psycopg` (pure-Python mode over the system libpq), not `psycopg[binary]`: the binary wheel's
# MAGIC bundled libpq aborts the process (SIGABRT) on ARM serverless hosts, e.g. Databricks Free Edition.

# COMMAND ----------

# MAGIC %pip install "psycopg>=3.0" "databricks-sdk>=0.81.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "lakebase_demo")
dbutils.widgets.text("lakebase_project", "osc-lakebase-demo")
dbutils.widgets.text("lakebase_db", "osc_demo")
dbutils.widgets.text("app_name", "osc-lakebase-demo")
dbutils.widgets.text("app_source_path", "")
dbutils.widgets.text("warehouse_name", "osc-lakebase-demo-wh")
dbutils.widgets.text("olap_periods", "50")
dbutils.widgets.text("dab_max_deployments", "5")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
project_name = dbutils.widgets.get("lakebase_project")
lakebase_db = dbutils.widgets.get("lakebase_db")
app_name = dbutils.widgets.get("app_name")
app_source_path = dbutils.widgets.get("app_source_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Discovered Values from Upstream Tasks

# COMMAND ----------

# Read values discovered by upstream tasks
warehouse_id = dbutils.jobs.taskValues.get(
    taskKey="01_setup_lakehouse", key="warehouse_id"
)
lakebase_host = dbutils.jobs.taskValues.get(
    taskKey="02_setup_lakebase", key="lakebase_host"
)
lakebase_endpoint = dbutils.jobs.taskValues.get(
    taskKey="02_setup_lakebase", key="lakebase_endpoint"
)

print(f"Warehouse ID (from task 01): {warehouse_id}")
print(f"Lakebase host (from task 02): {lakebase_host}")
print(f"Lakebase endpoint (from task 02): {lakebase_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover App Service Principal

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
me = w.current_user.me()
print(f"Running as: {me.user_name}")

app = w.apps.get(name=app_name)
sp_id = str(app.service_principal_id)
print(f"App: {app_name}")
print(f"App SP ID: {sp_id}")

# The app carries its SP's client ID; the SCIM lookup (w.service_principals.get) is admin-only
sp_client_id = app.service_principal_client_id
print(f"App SP Client ID: {sp_client_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the Lakebase OAuth Role for SP

# COMMAND ----------

from databricks.sdk.service.postgres import (
    Role, RoleRoleSpec, RoleAuthMethod, RoleIdentityType
)

existing_roles = list(w.postgres.list_roles(
    parent=f"projects/{project_name}/branches/production"
))

sp_role_exists = False
for role in existing_roles:
    if (hasattr(role, 'status') and role.status
        and getattr(role.status, 'postgres_role', None) == sp_client_id
        and getattr(role.status, 'auth_method', None) == RoleAuthMethod.LAKEBASE_OAUTH_V1):
        print(f"OAuth role already exists for SP: {role.name}")
        sp_role_exists = True
        break

if not sp_role_exists:
    # Clean up any stale NO_LOGIN roles for this SP
    for role in existing_roles:
        if (hasattr(role, 'status') and role.status
            and getattr(role.status, 'postgres_role', None) == sp_client_id):
            print(f"Deleting stale role: {role.name}")
            w.postgres.delete_role(name=role.name).wait()

    # Created below in SQL with databricks_create_role(), not with w.postgres.create_role(): a role created
    # through the API cannot be granted to `authenticator` (no ADMIN option), which the Data API needs.
    print(f"OAuth role for SP {sp_client_id} will be created with databricks_create_role()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant the App SP Can Manage on the Lakebase Project
# MAGIC The branching, CI/CD, point-in-time restore, scale-to-zero and autoscaling demos create branches and
# MAGIC endpoints and change compute settings, which needs project-level CAN_MANAGE.

# COMMAND ----------

from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

# The permissions API addresses Lakebase projects by project ID (name), not by UID
w.permissions.update(
    request_object_type="database-projects",
    request_object_id=project_name,
    access_control_list=[AccessControlRequest(service_principal_name=sp_client_id,
                                              permission_level=PermissionLevel.CAN_MANAGE)],
)
print(f"Granted CAN_MANAGE on Lakebase project {project_name} to SP {sp_client_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant the App SP the SQL Warehouse and Unity Catalog Schema
# MAGIC The DBSQL demos query, edit and create Delta tables as the app SP. USE CATALOG on the catalog is a one-time
# MAGIC grant by a catalog manager if whoever runs this job does not own the catalog; see README.

# COMMAND ----------

w.permissions.update(
    request_object_type="warehouses",
    request_object_id=warehouse_id,
    access_control_list=[AccessControlRequest(service_principal_name=sp_client_id,
                                              permission_level=PermissionLevel.CAN_USE)],
)
spark.sql(f"GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA {catalog}.{schema} TO `{sp_client_id}`")
print(f"Granted CAN_USE on warehouse {warehouse_id} and schema privileges on {catalog}.{schema} to SP {sp_client_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Schema and Table Access to SP

# COMMAND ----------

import psycopg
import socket

cred = w.postgres.generate_database_credential(endpoint=lakebase_endpoint)
username = me.user_name or me.display_name

try:
    ip = socket.gethostbyname(lakebase_host)
except Exception:
    ip = lakebase_host

conn = psycopg.connect(
    host=lakebase_host, hostaddr=ip, dbname=lakebase_db,
    user=username, password=cred.token, sslmode="require",
)
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp_client_id,))
if not cur.fetchone():
    cur.execute("SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')", (sp_client_id,))
    print(f"Created OAuth role for SP {sp_client_id}: {cur.fetchone()[0]}")

# CREATE: the app creates its own tables (schema_migrations, advisor_risk_scores, OLAP/reverse-ETL demo tables);
# granted on production, so branches created later inherit it
cur.execute(f'GRANT USAGE, CREATE ON SCHEMA lakebase_demo TO "{sp_client_id}"')
cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA lakebase_demo TO "{sp_client_id}"')
cur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA lakebase_demo GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "{sp_client_id}"')

# Data API (PostgREST): it connects as `authenticator` and switches to the caller's role, so the SP role must be
# granted to authenticator. The authenticator role only exists once the Data API is enabled for this database.
cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'authenticator'")
if cur.fetchone():
    cur.execute(f'GRANT "{sp_client_id}" TO authenticator')
    print(f"Data API: granted SP role {sp_client_id} to authenticator")
else:
    print("Data API not enabled for this database (no authenticator role); re-run setup after enabling it")

# Row-level security demo: one NOLOGIN role per district office; the app SP may SET ROLE to each of them.
# region_filter shows a role only its district's advisors; the app SP itself keeps full access (other demos write here).
RLS_REGIONS = ["Hamilton", "Kingston", "London", "Mississauga", "Ottawa", "Sudbury", "Toronto", "Windsor"]  # = app/main.py
for region in RLS_REGIONS:
    role = f"rls_{region.lower()}"
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    if not cur.fetchone():
        cur.execute(f'CREATE ROLE "{role}" NOLOGIN')
    cur.execute(f'GRANT USAGE ON SCHEMA lakebase_demo TO "{role}"')
    cur.execute(f'GRANT SELECT ON lakebase_demo.advisors TO "{role}"')
    cur.execute(f'GRANT "{role}" TO "{sp_client_id}"')
cur.execute("ALTER TABLE lakebase_demo.advisors ENABLE ROW LEVEL SECURITY")
cur.execute("DROP POLICY IF EXISTS region_filter ON lakebase_demo.advisors")
cur.execute("CREATE POLICY region_filter ON lakebase_demo.advisors FOR SELECT USING (LOWER(region) = REPLACE(current_user, 'rls_', ''))")
cur.execute("DROP POLICY IF EXISTS app_full_access ON lakebase_demo.advisors")
cur.execute(f'CREATE POLICY app_full_access ON lakebase_demo.advisors TO "{sp_client_id}" USING (true) WITH CHECK (true)')
print(f"RLS: {len(RLS_REGIONS)} district roles granted to the app SP; region_filter and app_full_access policies on advisors")

print(f"Granted full access on lakebase_demo to SP: {sp_client_id}")

cur.execute("SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = %s", (sp_client_id,))
print(f"Postgres role: {cur.fetchone()}")
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update App Environment Variables
# MAGIC The app reads its names from the bundle and looks up the warehouse id and Lakebase host when it starts,
# MAGIC so all this task does is (re)deploy it now that the warehouse, project and grants exist.

# COMMAND ----------

from databricks.sdk.service.apps import AppDeployment, ComputeState, EnvVar

# Same command and env as resources/osc_app.app.yml (`config`), which only the CLI applies; keep them in sync
app_command = ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
app_env = {
    "UC_CATALOG": catalog,
    "UC_SCHEMA": schema,
    "WAREHOUSE_NAME": dbutils.widgets.get("warehouse_name"),
    "LAKEBASE_PROJECT": project_name,
    "LAKEBASE_DB": lakebase_db,
    "OLAP_PERIODS": dbutils.widgets.get("olap_periods"),
    "DAB_MAX_DEPLOYMENTS": dbutils.widgets.get("dab_max_deployments"),
}

app = w.apps.get(name=app_name)
if not app.compute_status or app.compute_status.state != ComputeState.ACTIVE:
    print(f"Starting app compute for {app_name}...")
    w.apps.start(name=app_name).result()
print(f"Deploying {app_name} from {app_source_path}...")
deployment = w.apps.deploy(app_name=app_name, app_deployment=AppDeployment(
    source_code_path=app_source_path, command=app_command,
    env_vars=[EnvVar(name=k, value=v) for k, v in app_env.items()])).result()
print(f"Deployment {deployment.deployment_id}: {deployment.status.state} - {deployment.status.message}")
print(f"App URL: {w.apps.get(name=app_name).url}")

# COMMAND ----------

print(f"\n{'='*50}")
print("App Configuration Complete")
print(f"{'='*50}")
print(f"App: {app_name}")
print(f"SP: {sp_client_id}")
print(f"Warehouse: {warehouse_id}")
print(f"Lakebase: {lakebase_host} / {lakebase_db}")
print(f"UC: {catalog}.{schema}")
