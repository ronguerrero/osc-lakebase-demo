"""Databricks App: Lakehouse vs Lakebase Performance Comparison"""
import os
import time
import socket
from typing import Any
from decimal import Decimal

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient

app = FastAPI(title="OSC Registrant Oversight - Lakebase Demos")
w = WorkspaceClient()

# Config: names come from the bundle (resources/osc_app.app.yml); IDs and hosts are looked up at startup,
# so redeploying never needs a generated app.yaml. Any value can still be pinned with its env var.
LAKEBASE_DB = os.environ["LAKEBASE_DB"]
LAKEBASE_SCHEMA = os.getenv("LAKEBASE_SCHEMA", "lakebase_demo")
UC_CATALOG = os.environ["UC_CATALOG"]
UC_SCHEMA = os.environ["UC_SCHEMA"]
LAKEBASE_ENDPOINT = (os.getenv("LAKEBASE_ENDPOINT")
                     or f"projects/{os.environ['LAKEBASE_PROJECT']}/branches/production/endpoints/primary")


def _lookup(what: str, fn) -> str:
    try:
        return fn()
    except Exception as e:  # setup job not run yet: start anyway, the setup job restarts the app when done
        print(f"Could not look up {what} ({e}); run the setup job, which restarts the app")
        return ""


def _find_warehouse_id() -> str:
    name = os.environ["WAREHOUSE_NAME"]
    return next(wh.id for wh in w.warehouses.list() if wh.name == name)


DBSQL_WAREHOUSE_ID = os.getenv("DBSQL_WAREHOUSE_ID") or _lookup("SQL warehouse", _find_warehouse_id)
LAKEBASE_HOST = os.getenv("LAKEBASE_HOST") or _lookup(
    "Lakebase endpoint", lambda: w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT).status.hosts.host)
LAKEBASE_DATA_API_URL = os.getenv("LAKEBASE_DATA_API_URL") or (
    f"https://{LAKEBASE_HOST}/api/2.0/workspace/{w.get_workspace_id()}/rest/{LAKEBASE_DB}" if LAKEBASE_HOST else "")
# OLAP demo table = 100K contributions x OLAP_PERIODS monthly periods. 50 (5M rows) needs ~1.5 GB in Lakebase;
# use fewer on small instances (Databricks Free Edition caps a Lakebase project at 512 MB).
OLAP_PERIODS = int(os.getenv("OLAP_PERIODS", "50"))
OLAP_LABEL = f"{OLAP_PERIODS * 100_000 / 1_000_000:g}M"

TABLES = [
    "account_types", "advisors", "investors", "accounts", "holdings",
    "beneficiaries", "kyc_profiles", "contributions", "complaints", "restitution_payments",
]

VALID_SOURCES = "^(dbsql_statement|dbsql_connector|lakebase_pg|lakebase_dataapi)$"


class TableDataResponse(BaseModel):
    table: str
    columns: list[str]
    rows: list[dict[str, Any]]
    total_count: int
    page: int
    page_size: int
    elapsed_ms: float
    source: str


class UpdateRequest(BaseModel):
    table: str
    pk_column: str
    pk_value: Any
    column: str
    value: Any


class UpdateResponse(BaseModel):
    success: bool
    elapsed_ms: float
    source: str


# --- DBSQL via Statement Execution API ---


def query_dbsql(sql: str) -> tuple[list[str], list[dict]]:
    """Execute SQL via Databricks SDK Statement Execution API."""
    from databricks.sdk.service.sql import StatementState

    resp = w.statement_execution.execute_statement(
        warehouse_id=DBSQL_WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )
    if resp.status and resp.status.state == StatementState.FAILED:
        error_msg = resp.status.error.message if resp.status.error else "Unknown error"
        raise HTTPException(500, f"DBSQL error: {error_msg}")

    manifest = resp.manifest
    result = resp.result
    if not manifest or not manifest.schema or not manifest.schema.columns:
        return [], []

    columns = [col.name for col in manifest.schema.columns]
    rows = []
    if result and result.data_array:
        for row_data in result.data_array:
            row = {}
            for i, col in enumerate(columns):
                val = row_data[i] if i < len(row_data) else None
                if val is not None:
                    try:
                        if "." in val:
                            val = float(val)
                        else:
                            val = int(val)
                    except (ValueError, TypeError):
                        pass
                    if val == "true":
                        val = True
                    elif val == "false":
                        val = False
                row[col] = val
            rows.append(row)
    return columns, rows


# --- DBSQL via JDBC/ODBC Connector ---


def query_dbsql_connector(sql: str) -> tuple[list[str], list[dict]]:
    """Execute SQL via databricks-sql-connector (Thrift/JDBC)."""
    from databricks.sql import connect as dbsql_connect

    host = w.config.host.replace("https://", "")
    token_header = w.config.authenticate()
    token = token_header.get("Authorization", "").replace("Bearer ", "")

    conn = dbsql_connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{DBSQL_WAREHOUSE_ID}",
        access_token=token,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        raw_rows = cursor.fetchall()
        rows = []
        for raw in raw_rows:
            row = {}
            for i, col in enumerate(columns):
                val = raw[i]
                if isinstance(val, Decimal):
                    val = float(val)
                row[col] = val
            rows.append(row)
        return columns, rows
    finally:
        conn.close()


def _update_endpoint_or_explain(**kwargs):
    """w.postgres.update_endpoint, but compute settings the workspace tier does not allow (e.g. Databricks Free
    Edition fixes autoscaling limits and the suspend timeout) come back as a readable 409 instead of a 500."""
    from databricks.sdk.errors import BadRequest, PermissionDenied
    try:
        return w.postgres.update_endpoint(**kwargs).wait()
    except (BadRequest, PermissionDenied) as e:
        raise HTTPException(409, f"Not available on this workspace: {e}")


# --- Lakebase via psycopg (Postgres wire protocol) ---


def _get_pg_connection():
    import psycopg

    cred = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
    me = w.current_user.me()
    username = me.user_name or me.display_name
    host = LAKEBASE_HOST
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host
    return psycopg.connect(
        host=host, hostaddr=ip, dbname=LAKEBASE_DB,
        user=username, password=cred.token, sslmode="require",
    )


def query_lakebase(sql: str) -> tuple[list[str], list[dict]]:
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = []
            for raw in cur.fetchall():
                row = {}
                for i, col in enumerate(columns):
                    val = raw[i]
                    if isinstance(val, Decimal):
                        val = float(val)
                    row[col] = val
                rows.append(row)
            return columns, rows
    finally:
        conn.close()


def execute_lakebase(sql: str, params: tuple | None = None) -> int:
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


# --- Lakebase Data API (PostgREST) ---


def _get_oauth_token() -> str:
    """Get OAuth token for Data API requests."""
    headers = w.config.authenticate()
    return headers.get("Authorization", "").replace("Bearer ", "")


def query_lakebase_dataapi(
    table: str, pk_col: str, page_size: int, offset: int, user_token: str = ""
) -> tuple[list[str], list[dict], int]:
    """Query via Lakebase Data API (PostgREST). Returns columns, rows, total_count."""
    token = user_token or _get_oauth_token()
    base = LAKEBASE_DATA_API_URL.rstrip("/")
    url = f"{base}/{LAKEBASE_SCHEMA}/{table}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Accept-Profile": LAKEBASE_SCHEMA,  # PostgREST schema selection (reads)
        "Prefer": "count=exact",
    }
    params = {
        "order": f"{pk_col}.asc",
        "limit": str(page_size),
        "offset": str(offset),
    }

    resp = httpx.get(url, headers=headers, params=params, timeout=30.0)
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, f"Data API error: {resp.text}")

    rows = resp.json()
    total = 0
    content_range = resp.headers.get("Content-Range", "")
    if "/" in content_range:
        total_str = content_range.split("/")[-1]
        if total_str != "*":
            total = int(total_str)

    columns = list(rows[0].keys()) if rows else []
    return columns, rows, total


def update_lakebase_dataapi(
    table: str, pk_col: str, pk_val: Any, column: str, value: Any, user_token: str = ""
) -> None:
    """Update a record via Lakebase Data API."""
    token = user_token or _get_oauth_token()
    base = LAKEBASE_DATA_API_URL.rstrip("/")
    url = f"{base}/{LAKEBASE_SCHEMA}/{table}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Content-Profile": LAKEBASE_SCHEMA,  # PostgREST schema selection (writes)
    }
    params = {f"{pk_col}": f"eq.{pk_val}"}
    body = {column: value}

    resp = httpx.patch(url, headers=headers, params=params, json=body, timeout=30.0)
    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, f"Data API update error: {resp.text}")


# --- Primary key mapping ---

PK_MAP = {
    "account_types": "account_type_id",
    "advisors": "advisor_id",
    "investors": "investor_id",
    "accounts": "account_id",
    "holdings": "holding_id",
    "beneficiaries": "beneficiary_id",
    "kyc_profiles": "kyc_id",
    "contributions": "contribution_id",
    "complaints": "complaint_id",
    "restitution_payments": "payment_id",
}


# --- API Endpoints ---

@app.get("/api/tables")
async def list_tables():
    return {"tables": TABLES}


@app.get("/api/data", response_model=TableDataResponse)
async def get_table_data(
    request: Request,
    table: str,
    source: str = Query(..., pattern=VALID_SOURCES),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    if table not in TABLES:
        raise HTTPException(404, f"Table '{table}' not found")

    offset = (page - 1) * page_size
    pk = PK_MAP[table]

    if source == "dbsql_statement":
        fqn = f"{UC_CATALOG}.{UC_SCHEMA}.{table}"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn}"
        data_sql = f"SELECT * FROM {fqn} ORDER BY {pk} LIMIT {page_size} OFFSET {offset}"
        start = time.time()
        _, count_rows = query_dbsql(count_sql)
        total = count_rows[0]["cnt"]
        columns, rows = query_dbsql(data_sql)
        elapsed = (time.time() - start) * 1000

    elif source == "dbsql_connector":
        fqn = f"{UC_CATALOG}.{UC_SCHEMA}.{table}"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {fqn}"
        data_sql = f"SELECT * FROM {fqn} ORDER BY {pk} LIMIT {page_size} OFFSET {offset}"
        start = time.time()
        _, count_rows = query_dbsql_connector(count_sql)
        total = count_rows[0]["cnt"]
        columns, rows = query_dbsql_connector(data_sql)
        elapsed = (time.time() - start) * 1000

    elif source == "lakebase_pg":
        schema_table = f"{LAKEBASE_SCHEMA}.{table}"
        count_sql = f"SELECT COUNT(*) AS cnt FROM {schema_table}"
        data_sql = f"SELECT * FROM {schema_table} ORDER BY {pk} LIMIT {page_size} OFFSET {offset}"
        start = time.time()
        _, count_rows = query_lakebase(count_sql)
        total = count_rows[0]["cnt"]
        columns, rows = query_lakebase(data_sql)
        elapsed = (time.time() - start) * 1000

    else:  # lakebase_dataapi
        user_token = ""  # Use SP token; DB owner can't use Data API directly
        start = time.time()
        columns, rows, total = query_lakebase_dataapi(table, pk, page_size, offset, user_token)
        elapsed = (time.time() - start) * 1000

    # Convert non-serializable types
    for row in rows:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
            elif isinstance(v, Decimal):
                row[k] = float(v)
            elif isinstance(v, bytes):
                row[k] = v.hex()

    return TableDataResponse(
        table=table,
        columns=columns,
        rows=rows,
        total_count=total,
        page=page,
        page_size=page_size,
        elapsed_ms=round(elapsed, 2),
        source=source,
    )


@app.post("/api/update", response_model=UpdateResponse)
async def update_record(request: Request, req: UpdateRequest, source: str = Query(..., pattern=VALID_SOURCES)):
    if req.table not in TABLES:
        raise HTTPException(404, f"Table '{req.table}' not found")

    if source in ("dbsql_statement", "dbsql_connector"):
        fqn = f"{UC_CATALOG}.{UC_SCHEMA}.{req.table}"
        val = f"'{req.value}'" if isinstance(req.value, str) else ("NULL" if req.value is None else str(req.value))
        pk_val = f"'{req.pk_value}'" if isinstance(req.pk_value, str) else str(req.pk_value)
        sql = f"UPDATE {fqn} SET {req.column} = {val} WHERE {req.pk_column} = {pk_val}"
        start = time.time()
        if source == "dbsql_statement":
            query_dbsql(sql)
        else:
            query_dbsql_connector(sql)
        elapsed = (time.time() - start) * 1000

    elif source == "lakebase_pg":
        schema_table = f"{LAKEBASE_SCHEMA}.{req.table}"
        sql = f"UPDATE {schema_table} SET {req.column} = %s WHERE {req.pk_column} = %s"
        start = time.time()
        execute_lakebase(sql, (req.value, req.pk_value))
        elapsed = (time.time() - start) * 1000

    else:  # lakebase_dataapi
        user_token = ""  # Use SP token; DB owner can't use Data API directly
        start = time.time()
        update_lakebase_dataapi(req.table, req.pk_column, req.pk_value, req.column, req.value, user_token)
        elapsed = (time.time() - start) * 1000

    return UpdateResponse(success=True, elapsed_ms=round(elapsed, 2), source=source)


# --- Analytics Benchmark (OLTP vs OLAP) ---
#
# OLTP queries run on BOTH engines (same small dataset) to show Lakebase's
# low-latency advantage for transactional patterns.
#
# OLAP queries run on DBSQL ONLY against a large 5M-row table that represents
# historical data living in the Lakehouse — data that doesn't belong in an
# operational database.

OLAP_TABLE = "account_transactions"
OLAP_FQN = f"{UC_CATALOG}.{UC_SCHEMA}.{OLAP_TABLE}"

ANALYTICS_QUERIES = [
    # --- OLTP: Where Lakebase shines (run on both engines) ---
    {
        "id": "point_lookup",
        "name": "Point Lookup",
        "description": "Single-row fetch by primary key",
        "category": "OLTP",
        "section": "oltp",
        "sql_template": "SELECT * FROM {prefix}advisors WHERE advisor_id = 42",
    },
    {
        "id": "filtered_pagination",
        "name": "Filtered Pagination",
        "description": "Active high-value investor accounts (WHERE + ORDER BY + LIMIT)",
        "category": "OLTP",
        "section": "oltp",
        "sql_template": (
            "SELECT account_id, account_number, market_value, status, open_date "
            "FROM {prefix}accounts "
            "WHERE status = 'ACTIVE' AND market_value > 150000 "
            "ORDER BY market_value DESC LIMIT 20"
        ),
    },
    {
        "id": "small_aggregation",
        "name": "Dashboard Summary",
        "description": "Lightweight GROUP BY on 500-row advisors table",
        "category": "OLTP",
        "section": "oltp",
        "sql_template": (
            "SELECT region, COUNT(*) AS advisor_count "
            "FROM {prefix}advisors GROUP BY region ORDER BY advisor_count DESC"
        ),
    },
    # --- OLAP: Where Lakehouse shines (DBSQL only, 5M-row table) ---
    {
        "id": "monthly_revenue",
        "name": "Monthly Revenue Trend",
        "description": f"Aggregate {OLAP_LABEL} transactions by year and month",
        "category": "Full Scan",
        "section": "olap",
        "sql_template": (
            "SELECT txn_year, txn_month, "
            "COUNT(*) AS transactions, "
            "ROUND(SUM(amount), 2) AS total_revenue, "
            "ROUND(AVG(amount), 2) AS avg_amount "
            "FROM {prefix}account_transactions "
            "GROUP BY txn_year, txn_month "
            "ORDER BY txn_year, txn_month"
        ),
    },
    {
        "id": "channel_analysis",
        "name": "Channel & Status Breakdown",
        "description": f"Multi-dimensional GROUP BY across {OLAP_LABEL} transactions",
        "category": "Aggregation",
        "section": "olap",
        "sql_template": (
            "SELECT channel, txn_status, payment_method, "
            "COUNT(*) AS txn_count, "
            "ROUND(SUM(amount), 2) AS total_amount, "
            "ROUND(AVG(amount), 2) AS avg_amount "
            "FROM {prefix}account_transactions "
            "GROUP BY channel, txn_status, payment_method "
            "ORDER BY total_amount DESC"
        ),
    },
    {
        "id": "top_accounts_revenue",
        "name": "Top Accounts by Lifetime Revenue",
        "description": f"JOIN {OLAP_LABEL} transactions with 100K accounts, rank by revenue",
        "category": "JOIN + Scan",
        "section": "olap",
        "sql_template": (
            "SELECT p.account_id, p.account_number, p.status, "
            "COUNT(*) AS transaction_count, "
            "ROUND(SUM(t.amount), 2) AS total_revenue "
            "FROM {prefix}account_transactions t "
            "JOIN {prefix}accounts p ON t.account_id = p.account_id "
            "GROUP BY p.account_id, p.account_number, p.status "
            "ORDER BY total_revenue DESC LIMIT 20"
        ),
    },
]


def _serialize_rows(rows: list[dict]) -> list[dict]:
    """Convert non-JSON-serializable types in query result rows."""
    for row in rows:
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
            elif isinstance(v, Decimal):
                row[k] = float(v)
            elif isinstance(v, bytes):
                row[k] = v.hex()
    return rows


def _dbsql_execute_and_wait(sql: str) -> None:
    """Execute a DBSQL statement and wait for completion (for DDL/setup queries)."""
    from databricks.sdk.service.sql import StatementState

    resp = w.statement_execution.execute_statement(
        warehouse_id=DBSQL_WAREHOUSE_ID,
        statement=sql,
        wait_timeout="50s",
    )

    if resp.status:
        if resp.status.state == StatementState.FAILED:
            raise HTTPException(500, f"DBSQL error: {resp.status.error.message if resp.status.error else 'Unknown'}")
        if resp.status.state == StatementState.SUCCEEDED:
            return

    # Still running — poll until complete
    stmt_id = resp.statement_id
    for _ in range(30):
        time.sleep(2)
        poll = w.statement_execution.get_statement(stmt_id)
        if poll.status.state == StatementState.SUCCEEDED:
            return
        if poll.status.state == StatementState.FAILED:
            raise HTTPException(500, f"DBSQL error: {poll.status.error.message if poll.status.error else 'Unknown'}")

    raise HTTPException(500, "Query timed out")


@app.get("/api/analytics/queries")
async def list_analytics_queries():
    """List available benchmark queries with section info."""
    return {"queries": [
        {
            "id": q["id"], "name": q["name"], "description": q["description"],
            "category": q["category"], "section": q["section"],
        }
        for q in ANALYTICS_QUERIES
    ]}


@app.get("/api/analytics/olap-status")
async def olap_table_status():
    """Check if the OLAP table exists in both engines."""
    dbsql_count = 0
    lakebase_count = 0
    try:
        _, rows = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {OLAP_FQN}")
        dbsql_count = rows[0]["cnt"]
    except Exception:
        pass
    try:
        _, rows = query_lakebase(f"SELECT COUNT(*) AS cnt FROM {LAKEBASE_SCHEMA}.{OLAP_TABLE}")
        lakebase_count = rows[0]["cnt"]
    except Exception:
        pass
    ready = dbsql_count > 0 and lakebase_count > 0
    return {
        "label": OLAP_LABEL,
        "expected_rows": OLAP_PERIODS * 100_000,
        "exists": ready,
        "table": OLAP_FQN,
        "row_count": dbsql_count,
        "dbsql_count": dbsql_count,
        "lakebase_count": lakebase_count,
    }


def _setup_lakebase_olap_table() -> int:
    """Create and populate account_transactions in Lakebase via generate_series."""
    schema_table = f"{LAKEBASE_SCHEMA}.{OLAP_TABLE}"
    contributions_table = f"{LAKEBASE_SCHEMA}.contributions"

    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema_table} (
                    txn_id BIGINT PRIMARY KEY,
                    account_id INT NOT NULL,
                    amount NUMERIC(12,2) NOT NULL,
                    transaction_date DATE,
                    txn_year INT,
                    txn_month INT,
                    txn_status TEXT,
                    payment_method TEXT,
                    channel TEXT
                )
            """)
            conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM {schema_table}")
            if cur.fetchone()[0] > 0:
                cur.execute(f"SELECT COUNT(*) FROM {schema_table}")
                return cur.fetchone()[0]

            # Populate in batches of 500K rows (5 periods each) to keep transactions manageable
            for batch_start in range(0, OLAP_PERIODS, 5):
                batch_end = min(batch_start + 4, OLAP_PERIODS - 1)
                cur.execute(f"""
                    INSERT INTO {schema_table}
                    SELECT
                      CAST(p.contribution_id AS BIGINT) * 50 + s.n,
                      p.account_id,
                      ROUND((p.amount * (1.0 + (RANDOM() * 0.3 - 0.15)))::numeric, 2),
                      p.due_date + (s.n * 30),
                      EXTRACT(YEAR FROM p.due_date + (s.n * 30))::int,
                      EXTRACT(MONTH FROM p.due_date + (s.n * 30))::int,
                      CASE WHEN RANDOM() > 0.08 THEN 'COMPLETED'
                           WHEN RANDOM() > 0.5 THEN 'PENDING'
                           ELSE 'FAILED' END,
                      p.payment_method,
                      CASE WHEN RANDOM() > 0.7 THEN 'online'
                           WHEN RANDOM() > 0.4 THEN 'mobile'
                           ELSE 'branch' END
                    FROM {contributions_table} p
                    CROSS JOIN generate_series({batch_start}, {batch_end}) AS s(n)
                """)
                conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM {schema_table}")
            return cur.fetchone()[0]
    finally:
        conn.close()


@app.post("/api/analytics/setup-olap")
async def setup_olap_table():
    """Generate the 5M-row account_transactions table in both DBSQL and Lakebase."""
    # Check if both already exist
    dbsql_count = 0
    lb_count = 0
    try:
        _, rows = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {OLAP_FQN}")
        dbsql_count = rows[0]["cnt"]
    except Exception:
        pass
    try:
        _, rows = query_lakebase(f"SELECT COUNT(*) AS cnt FROM {LAKEBASE_SCHEMA}.{OLAP_TABLE}")
        lb_count = rows[0]["cnt"]
    except Exception:
        pass

    if dbsql_count > 0 and lb_count > 0:
        return {
            "status": "exists",
            "table": OLAP_FQN,
            "row_count": dbsql_count,
            "dbsql_count": dbsql_count,
            "lakebase_count": lb_count,
        }

    start = time.time()
    contributions_fqn = f"{UC_CATALOG}.{UC_SCHEMA}.contributions"

    # Step 1: Create in DBSQL (if needed)
    if dbsql_count == 0:
        _dbsql_execute_and_wait(f"""
            CREATE TABLE IF NOT EXISTS {OLAP_FQN}
            AS
            SELECT
              CAST(p.contribution_id AS BIGINT) * 50 + CAST(m.month_idx AS BIGINT) AS txn_id,
              p.account_id,
              ROUND(p.amount * (1.0 + (RAND() * 0.3 - 0.15)), 2) AS amount,
              DATE_ADD(p.due_date, CAST(m.month_idx * 30 AS INT)) AS transaction_date,
              YEAR(DATE_ADD(p.due_date, CAST(m.month_idx * 30 AS INT))) AS txn_year,
              MONTH(DATE_ADD(p.due_date, CAST(m.month_idx * 30 AS INT))) AS txn_month,
              CASE
                WHEN RAND() > 0.08 THEN 'COMPLETED'
                WHEN RAND() > 0.5 THEN 'PENDING'
                ELSE 'FAILED'
              END AS txn_status,
              p.payment_method,
              CASE
                WHEN RAND() > 0.7 THEN 'online'
                WHEN RAND() > 0.4 THEN 'mobile'
                ELSE 'branch'
              END AS channel
            FROM {contributions_fqn} p
            CROSS JOIN (SELECT EXPLODE(SEQUENCE(0, {OLAP_PERIODS - 1})) AS month_idx) m
        """)
        _, rows = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {OLAP_FQN}")
        dbsql_count = rows[0]["cnt"]

    # Step 2: Create in Lakebase (if needed)
    if lb_count == 0:
        lb_count = _setup_lakebase_olap_table()

    elapsed = (time.time() - start) * 1000

    return {
        "status": "created",
        "table": OLAP_FQN,
        "row_count": dbsql_count,
        "dbsql_count": dbsql_count,
        "lakebase_count": lb_count,
        "elapsed_ms": round(elapsed, 1),
    }


@app.post("/api/analytics/run")
async def run_analytics_query(query_id: str = Query(...)):
    """Run a benchmark query on both DBSQL and Lakebase, returning timing and results."""
    query = next((q for q in ANALYTICS_QUERIES if q["id"] == query_id), None)
    if not query:
        raise HTTPException(404, f"Query '{query_id}' not found")

    template = query["sql_template"]
    section = query["section"]
    results = {}

    # Run on DBSQL via Connector (Thrift/JDBC) for fair wire-protocol comparison
    dbsql_sql = template.format(prefix=f"{UC_CATALOG}.{UC_SCHEMA}.")
    try:
        start = time.time()
        cols, rows = query_dbsql_connector(dbsql_sql)
        elapsed = (time.time() - start) * 1000
        results["dbsql"] = {
            "sql": dbsql_sql.strip(),
            "columns": cols,
            "rows": _serialize_rows(rows)[:50],
            "row_count": len(rows),
            "elapsed_ms": round(elapsed, 1),
            "error": None,
        }
    except Exception as e:
        results["dbsql"] = {
            "sql": dbsql_sql.strip(), "columns": [], "rows": [],
            "row_count": 0, "elapsed_ms": 0, "error": str(e),
        }

    # Run on Lakebase
    lb_sql = template.format(prefix=f"{LAKEBASE_SCHEMA}.")
    try:
        start = time.time()
        cols, rows = query_lakebase(lb_sql)
        elapsed = (time.time() - start) * 1000
        results["lakebase"] = {
            "sql": lb_sql.strip(),
            "columns": cols,
            "rows": _serialize_rows(rows)[:50],
            "row_count": len(rows),
            "elapsed_ms": round(elapsed, 1),
            "error": None,
        }
    except Exception as e:
        results["lakebase"] = {
            "sql": lb_sql.strip(), "columns": [], "rows": [],
            "row_count": 0, "elapsed_ms": 0, "error": str(e),
        }

    return {
        "query_id": query_id,
        "name": query["name"],
        "description": query["description"],
        "category": query["category"],
        "section": section,
        "results": results,
    }


# --- Branching Demo ---

LAKEBASE_PROJECT = LAKEBASE_ENDPOINT.split("/")[1]  # e.g. "osc-lakebase-demo"
BRANCH_ID = "dev-demo"
BRANCH_NAME = f"projects/{LAKEBASE_PROJECT}/branches/{BRANCH_ID}"

# Mutable branch connection state (discovered dynamically)
_branch_state: dict[str, Any] = {"host": None, "endpoint": None}


def _get_branch_pg_connection():
    """Connect to the dev branch endpoint via psycopg."""
    import psycopg

    if not _branch_state["endpoint"]:
        raise HTTPException(400, "No active database branch. Create one first.")

    cred = w.postgres.generate_database_credential(endpoint=_branch_state["endpoint"])
    me = w.current_user.me()
    username = me.user_name or me.display_name
    host = _branch_state["host"]
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host
    return psycopg.connect(
        host=host, hostaddr=ip, dbname=LAKEBASE_DB,
        user=username, password=cred.token, sslmode="require",
    )


def _query_branch(sql: str) -> tuple[list[str], list[dict]]:
    conn = _get_branch_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = []
            for raw in cur.fetchall():
                row = {}
                for i, col in enumerate(columns):
                    val = raw[i]
                    if isinstance(val, Decimal):
                        val = float(val)
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    row[col] = val
                rows.append(row)
            return columns, rows
    finally:
        conn.close()


def _execute_branch(sql: str, params: tuple | None = None) -> int:
    conn = _get_branch_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount
    finally:
        conn.close()


def _discover_branch() -> bool:
    """Check if the dev-demo branch exists and discover its endpoint. Returns True if active."""
    try:
        w.postgres.get_branch(name=BRANCH_NAME)
    except Exception:
        # Branch is gone (e.g. deleted outside the app): drop the cached endpoint so the UI offers "Create" again
        _branch_state["host"] = _branch_state["endpoint"] = None
        return False
    if _branch_state["endpoint"]:
        return True
    # Branch exists, discover endpoint
    endpoints = list(w.postgres.list_endpoints(parent=BRANCH_NAME))
    if endpoints and endpoints[0].status and endpoints[0].status.hosts:
        _branch_state["host"] = endpoints[0].status.hosts.host
        _branch_state["endpoint"] = endpoints[0].name
        return True
    return False


@app.get("/api/branch/status")
async def branch_status():
    active = _discover_branch()
    return {"active": active, "branch": BRANCH_ID}


def _create_branch_with_endpoint():
    """Create the branch and discover its compute endpoint."""
    from databricks.sdk.service.postgres import Branch, BranchSpec, Endpoint, EndpointSpec, EndpointType

    # Create branch (auto-creates a default read-write endpoint)
    w.postgres.create_branch(
        parent=f"projects/{LAKEBASE_PROJECT}",
        branch=Branch(
            spec=BranchSpec(
                source_branch=f"projects/{LAKEBASE_PROJECT}/branches/production",
                no_expiry=True,
            )
        ),
        branch_id=BRANCH_ID,
    ).wait()

    # Try to create a compute endpoint; if one already exists (auto-created), skip
    try:
        w.postgres.create_endpoint(
            parent=BRANCH_NAME,
            endpoint=Endpoint(
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=1.0,
                    autoscaling_limit_max_cu=1.0,
                )
            ),
            endpoint_id="primary",
        ).wait()
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise

    # Discover the endpoint host (works with both auto-created and manually created endpoints)
    endpoints = list(w.postgres.list_endpoints(parent=BRANCH_NAME))
    if endpoints and endpoints[0].status and endpoints[0].status.hosts:
        _branch_state["host"] = endpoints[0].status.hosts.host
        _branch_state["endpoint"] = endpoints[0].name
    else:
        raise HTTPException(500, "Branch created but no endpoint found")


@app.post("/api/branch/create")
async def create_branch():
    """Create the dev-demo database branch from production."""
    if _discover_branch():
        return {"status": "exists", "branch": BRANCH_ID, "message": "Database branch already exists"}

    start = time.time()
    _create_branch_with_endpoint()
    elapsed = (time.time() - start) * 1000

    return {
        "status": "created",
        "branch": BRANCH_ID,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Database branch created in {elapsed/1000:.1f}s",
    }


@app.get("/api/branch/compare")
async def compare_branches(
    table: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    if table not in TABLES:
        raise HTTPException(404, f"Table '{table}' not found")
    if not _discover_branch():
        raise HTTPException(400, "No active database branch")

    pk = PK_MAP[table]
    offset = (page - 1) * page_size
    schema_table = f"{LAKEBASE_SCHEMA}.{table}"
    count_sql = f"SELECT COUNT(*) AS cnt FROM {schema_table}"
    data_sql = f"SELECT * FROM {schema_table} ORDER BY {pk} LIMIT {page_size} OFFSET {offset}"

    # Query production via psycopg
    start = time.time()
    _, prod_count = query_lakebase(count_sql)
    prod_cols, prod_rows = query_lakebase(data_sql)
    prod_ms = (time.time() - start) * 1000

    # Query branch via psycopg
    start = time.time()
    _, branch_count = _query_branch(count_sql)
    branch_cols, branch_rows = _query_branch(data_sql)
    branch_ms = (time.time() - start) * 1000

    return {
        "table": table,
        "pk": pk,
        "page": page,
        "page_size": page_size,
        "production": {
            "columns": prod_cols,
            "rows": prod_rows,
            "total_count": prod_count[0]["cnt"],
            "elapsed_ms": round(prod_ms, 2),
        },
        "branch": {
            "columns": branch_cols,
            "rows": branch_rows,
            "total_count": branch_count[0]["cnt"],
            "elapsed_ms": round(branch_ms, 2),
        },
    }


class BranchActionRequest(BaseModel):
    action: str  # "revalue_accounts", "delete_closed", "add_advisor"
    table: str = "contributions"


@app.post("/api/branch/action")
async def branch_action(req: BranchActionRequest):
    if not _discover_branch():
        raise HTTPException(400, "No active database branch")
    start = time.time()

    if req.action == "revalue_accounts":
        affected = _execute_branch(
            f"UPDATE {LAKEBASE_SCHEMA}.contributions SET amount = ROUND(amount * 1.10, 2)"
        )
        message = f"Raised all scheduled contributions by 10% ({affected} rows updated)"

    elif req.action == "delete_closed":
        affected = _execute_branch(
            f"DELETE FROM {LAKEBASE_SCHEMA}.accounts WHERE status = 'CLOSED'"
        )
        message = f"Deleted all closed accounts ({affected} rows removed)"

    elif req.action == "add_advisor":
        cols, rows = _query_branch(f"SELECT MAX(advisor_id) AS max_id FROM {LAKEBASE_SCHEMA}.advisors")
        next_id = (rows[0]["max_id"] or 0) + 1
        _execute_branch(
            f"INSERT INTO {LAKEBASE_SCHEMA}.advisors (advisor_id, first_name, last_name, email, phone, nrd_number, registration_date, region, is_active) "
            f"VALUES (%s, 'Demo', 'Advisor', 'demo.advisor@ontariowealth.ca', '416-555-0000', 'NRD-DEMO-001', CURRENT_DATE, 'Toronto', true)",
            (next_id,),
        )
        affected = 1
        message = f"Registered new advisor 'Demo Advisor' (ID: {next_id})"

    else:
        raise HTTPException(400, f"Unknown action: {req.action}")

    elapsed = (time.time() - start) * 1000
    return {"success": True, "message": message, "affected_rows": affected, "elapsed_ms": round(elapsed, 2)}


@app.post("/api/branch/reset")
async def reset_branch():
    """Delete and recreate the dev branch from production to reset all data."""
    start = time.time()

    # Clear state
    _branch_state["host"] = None
    _branch_state["endpoint"] = None

    # Delete endpoints first, then branch (avoids "endpoint already exists" race)
    try:
        for ep in w.postgres.list_endpoints(parent=BRANCH_NAME):
            try:
                w.postgres.delete_endpoint(name=ep.name).wait()
            except Exception:
                pass
    except Exception:
        pass

    try:
        w.postgres.delete_branch(name=BRANCH_NAME).wait()
    except Exception as e:
        if "NOT_FOUND" not in str(e):
            raise HTTPException(500, f"Failed to delete branch: {e}")

    # Small delay to ensure backend cleanup completes
    time.sleep(2)

    # Recreate branch + endpoint
    _create_branch_with_endpoint()

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Database branch reset from production in {elapsed/1000:.1f}s",
    }


# --- Reverse ETL Demo ---
# Uses the Lakebase Autoscaling Synced Tables API (POST /api/2.0/postgres/synced_tables)
# to sync Delta tables to Lakebase Postgres via a managed Spark Declarative Pipeline.
# The SDK doesn't expose these methods yet, so we call the REST API directly
# via the SDK's internal API client (same pattern the SDK uses).

RETL_TABLE = "analytics_output"
RETL_FQN = f"{UC_CATALOG}.{UC_SCHEMA}.{RETL_TABLE}"
RETL_SYNCED_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.{RETL_TABLE}_synced"
RETL_PG_TABLE = f"{UC_SCHEMA}.{RETL_TABLE}_synced"  # synced tables land in the Postgres schema named after their UC schema
RETL_PK = "id"
RETL_SYNCED_TABLE_RESOURCE = f"synced_tables/{RETL_SYNCED_TABLE}"

# Internal API client for calling the Postgres REST API directly
_pg_api = w.postgres._api


def _pg_do(method: str, path: str, **kwargs):
    """Call the Postgres REST API via the SDK's internal API client."""
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    return _pg_api.do(method, f"/api/2.0/postgres/{path}", headers=headers, **kwargs)


def _check_retl_source() -> bool:
    """Check if the source Delta table exists."""
    try:
        query_dbsql(f"DESCRIBE TABLE {RETL_FQN}")
        return True
    except Exception:
        return False


def _get_synced_table_status() -> dict[str, Any] | None:
    """Get the synced table status from the Postgres API. Returns None if not found."""
    try:
        res = _pg_do("GET", RETL_SYNCED_TABLE_RESOURCE)
        status = res.get("status", {})
        pipeline_id = status.get("pipeline_id")
        pipeline_url = None
        if pipeline_id:
            host = w.config.host.rstrip("/")
            pipeline_url = f"{host}#joblist/pipelines/{pipeline_id}"
        return {
            "exists": True,
            "state": status.get("detailed_state", "UNKNOWN"),
            "message": status.get("message", ""),
            "pipeline_id": pipeline_id,
            "pipeline_url": pipeline_url,
        }
    except Exception:
        return None


@app.get("/api/retl/status")
async def retl_status():
    """Check if source table and synced table exist, with sync status."""
    source_exists = _check_retl_source()
    synced_info = _get_synced_table_status()

    src_count = 0
    if source_exists:
        try:
            _, rows = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {RETL_FQN}")
            src_count = rows[0]["cnt"]
        except Exception:
            pass

    tgt_count = 0
    synced = synced_info is not None
    if synced:
        try:
            _, rows = query_lakebase(f"SELECT COUNT(*) AS cnt FROM {RETL_PG_TABLE}")
            tgt_count = rows[0]["cnt"]
        except Exception:
            pass

    sync_status = None
    if source_exists and synced and synced_info:
        state = synced_info["state"]
        # Map API states to user-friendly status
        if "PROVISIONING" in state:
            friendly = "PROVISIONING"
        elif "FAILED" in state:
            friendly = "FAILED"
        elif "ONLINE" in state:
            friendly = "SYNCED" if src_count == tgt_count else "SYNCING"
        else:
            friendly = state
        sync_status = {
            "state": friendly,
            "detailed_state": state,
            "message": synced_info["message"] or f"{tgt_count}/{src_count} rows synced",
            "pipeline_id": synced_info.get("pipeline_id"),
            "pipeline_url": synced_info.get("pipeline_url"),
        }
    elif source_exists:
        sync_status = {"state": "NOT_CREATED", "detailed_state": "NOT_CREATED", "message": "Synced table not created"}

    return {
        "source_exists": source_exists,
        "source_table": RETL_FQN,
        "source_count": src_count,
        "synced": synced,
        "target_table": RETL_PG_TABLE,
        "target_count": tgt_count,
        "sync_status": sync_status,
    }


@app.post("/api/retl/setup-source")
async def retl_setup_source():
    """Create the source Delta table with sample data."""
    start = time.time()

    query_dbsql(f"""
        CREATE TABLE IF NOT EXISTS {RETL_FQN} (
            id BIGINT,
            report_date DATE,
            region STRING,
            active_accounts INT,
            assets_under_admin DECIMAL(12,2),
            avg_complaint_amount DECIMAL(10,2)
        )
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    # Check if empty and populate
    _, rows = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {RETL_FQN}")
    if rows[0]["cnt"] == 0:
        query_dbsql(f"""
            INSERT INTO {RETL_FQN} VALUES
            (1, '2026-03-01', 'Toronto', 12450, 8234500000.00, 3245.50),
            (2, '2026-03-01', 'Ottawa', 9870, 6123400000.00, 2987.25),
            (3, '2026-03-01', 'Hamilton', 11200, 7456700000.00, 3102.80),
            (4, '2026-03-01', 'London', 15600, 9234500000.00, 3567.90),
            (5, '2026-03-01', 'Kingston', 8900, 5678900000.00, 2876.40),
            (6, '2026-03-02', 'Toronto', 12500, 8267800000.00, 3256.10),
            (7, '2026-03-02', 'Ottawa', 9920, 6156700000.00, 2995.50),
            (8, '2026-03-02', 'Hamilton', 11250, 7489000000.00, 3115.20),
            (9, '2026-03-02', 'London', 15650, 9278900000.00, 3578.30),
            (10, '2026-03-02', 'Kingston', 8950, 5701200000.00, 2889.60)
        """)

    elapsed = (time.time() - start) * 1000
    return {"success": True, "elapsed_ms": round(elapsed, 2), "message": "Source table created with 10 rows"}


@app.post("/api/retl/create-sync")
async def retl_create_sync():
    """Create a synced table via the Lakebase Autoscaling Synced Tables API."""
    existing = _get_synced_table_status()
    if existing:
        return {"status": "exists", "message": f"Synced table already exists (state: {existing['state']})"}

    start = time.time()

    res = _pg_do(
        "POST", "synced_tables",
        query={"synced_table_id": RETL_SYNCED_TABLE},
        body={
            "spec": {
                "source_table_full_name": RETL_FQN,
                "project": f"projects/{LAKEBASE_PROJECT}",
                "branch": f"projects/{LAKEBASE_PROJECT}/branches/production",
                "primary_key_columns": [RETL_PK],
                "scheduling_policy": "TRIGGERED",
                "postgres_database": LAKEBASE_DB,
                "create_database_objects_if_missing": True,
            }
        },
    )

    # Extract status from the long-running operation response
    response = res.get("response", {})
    status = response.get("status", {})
    state = status.get("detailed_state", "UNKNOWN")
    message = status.get("message", "Synced table created")

    elapsed = (time.time() - start) * 1000
    return {
        "status": "created",
        "elapsed_ms": round(elapsed, 2),
        "state": state,
        "message": f"Synced table created ({state}) in {elapsed/1000:.1f}s — {message}",
    }


@app.post("/api/retl/delete-sync")
async def retl_delete_sync():
    """Delete the synced table via the API, then drop the Postgres table."""
    existing = _get_synced_table_status()
    if not existing:
        return {"status": "not_found", "message": "No synced table found"}

    start = time.time()

    # Step 1: Delete the synced table from Unity Catalog (stops the pipeline)
    _pg_do("DELETE", RETL_SYNCED_TABLE_RESOURCE)

    # Step 2: Drop the Postgres table to free up space
    try:
        execute_lakebase(f"DROP TABLE IF EXISTS {RETL_PG_TABLE}")
    except Exception:
        pass  # Table may not exist yet if sync was still provisioning

    elapsed = (time.time() - start) * 1000
    return {"status": "deleted", "elapsed_ms": round(elapsed, 2), "message": "Synced table deleted and Postgres table dropped"}


@app.post("/api/retl/trigger-sync")
async def retl_trigger_sync():
    """Trigger a sync run on the pipeline backing the synced table."""
    synced_info = _get_synced_table_status()
    if not synced_info:
        raise HTTPException(400, "No synced table found. Create one first.")

    pipeline_id = synced_info.get("pipeline_id")
    if not pipeline_id:
        raise HTTPException(400, "Pipeline not yet provisioned. Wait for initial sync to complete.")

    start = time.time()
    w.pipelines.start_update(pipeline_id=pipeline_id)

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Sync triggered on pipeline {pipeline_id}",
    }


@app.get("/api/retl/compare")
async def retl_compare(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    """Compare source Delta table and target Lakebase table."""
    offset = (page - 1) * page_size

    # Query source via DBSQL
    start = time.time()
    _, src_count = query_dbsql(f"SELECT COUNT(*) AS cnt FROM {RETL_FQN}")
    src_cols, src_rows = query_dbsql(
        f"SELECT * FROM {RETL_FQN} ORDER BY {RETL_PK} LIMIT {page_size} OFFSET {offset}"
    )
    src_ms = (time.time() - start) * 1000

    # Query target via psycopg
    tgt_cols, tgt_rows, tgt_total, tgt_ms = [], [], 0, 0.0
    synced_info = _get_synced_table_status()
    if synced_info:
        try:
            start = time.time()
            _, tgt_count = query_lakebase(f"SELECT COUNT(*) AS cnt FROM {RETL_PG_TABLE}")
            tgt_cols, tgt_rows = query_lakebase(
                f"SELECT * FROM {RETL_PG_TABLE} ORDER BY {RETL_PK} LIMIT {page_size} OFFSET {offset}"
            )
            tgt_total = tgt_count[0]["cnt"]
            tgt_ms = (time.time() - start) * 1000
            for row in tgt_rows:
                for k, v in row.items():
                    if hasattr(v, "isoformat"):
                        row[k] = v.isoformat()
                    elif isinstance(v, Decimal):
                        row[k] = float(v)
        except Exception:
            pass

    return {
        "source": {
            "columns": src_cols,
            "rows": src_rows,
            "total_count": src_count[0]["cnt"],
            "elapsed_ms": round(src_ms, 2),
        },
        "target": {
            "columns": tgt_cols,
            "rows": tgt_rows,
            "total_count": tgt_total,
            "elapsed_ms": round(tgt_ms, 2),
        },
        "page": page,
        "page_size": page_size,
    }


@app.post("/api/retl/insert")
async def retl_insert():
    """Insert a new row into the source Delta table."""
    import random

    start = time.time()

    _, rows = query_dbsql(f"SELECT MAX(id) AS max_id FROM {RETL_FQN}")
    next_id = (rows[0]["max_id"] or 0) + 1

    regions = ["Toronto", "Ottawa", "Hamilton", "London", "Kingston"]
    region = regions[(next_id - 1) % len(regions)]

    accounts = random.randint(5000, 20000)
    assets = round(random.uniform(3000000000, 9500000000), 2)
    avg_complaint = round(random.uniform(2000, 4000), 2)

    query_dbsql(f"""
        INSERT INTO {RETL_FQN} VALUES
        ({next_id}, CURRENT_DATE(), '{region}', {accounts}, {assets}, {avg_complaint})
    """)

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Inserted row {next_id} ({region}, {accounts} accounts, ${assets:,.0f} assets under administration)",
    }


# --- Scale-to-Zero Demo ---

# Query latency history: list of {elapsed_ms, state, timestamp}
_s2z_history: list[dict[str, Any]] = []


def _get_endpoint_info() -> dict[str, Any]:
    """Get the production endpoint status and config."""
    ep = w.postgres.get_endpoint(name=LAKEBASE_ENDPOINT)
    status = ep.status
    spec = ep.spec

    # Determine state
    state = "UNKNOWN"
    if status and status.current_state:
        state = str(status.current_state.value) if hasattr(status.current_state, "value") else str(status.current_state)
        # Clean up enum prefix
        state = state.replace("ENDPOINT_STATE_", "")

    # Suspend timeout — lives in status, not spec
    suspend_timeout = 0
    for src in [status, spec]:
        if src and getattr(src, "suspend_timeout_duration", None):
            raw = src.suspend_timeout_duration
            if hasattr(raw, "seconds"):
                suspend_timeout = int(raw.seconds)
            elif isinstance(raw, str) and raw.endswith("s"):
                suspend_timeout = int(raw.rstrip("s"))
            elif isinstance(raw, (int, float)):
                suspend_timeout = int(raw)
            if suspend_timeout > 0:
                break

    return {
        "state": state,
        "endpoint_name": ep.name,
        "min_cu": float(status.autoscaling_limit_min_cu) if status and status.autoscaling_limit_min_cu else 0,
        "max_cu": float(status.autoscaling_limit_max_cu) if status and status.autoscaling_limit_max_cu else 0,
        "suspend_timeout_seconds": suspend_timeout,
        "scale_to_zero_enabled": suspend_timeout > 0,
    }


@app.get("/api/s2z/status")
async def s2z_status():
    """Get endpoint state, config, and query history."""
    info = _get_endpoint_info()
    return {
        **info,
        "history": _s2z_history[-20:],  # last 20 queries
    }


@app.post("/api/s2z/configure")
async def s2z_configure(timeout_seconds: int = Query(..., ge=0)):
    """Set the scale-to-zero suspend timeout. 0 = disabled (always active)."""
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, FieldMask
    from google.protobuf.duration_pb2 import Duration

    start = time.time()

    # Get current endpoint config to preserve CU settings
    current = _get_endpoint_info()

    spec_kwargs: dict[str, Any] = {
        "endpoint_type": EndpointType.ENDPOINT_TYPE_READ_WRITE,
        "autoscaling_limit_min_cu": current["min_cu"] or 1.0,
        "autoscaling_limit_max_cu": current["max_cu"] or 1.0,
    }
    if timeout_seconds > 0:
        spec_kwargs["suspend_timeout_duration"] = Duration(seconds=timeout_seconds)
    else:
        spec_kwargs["no_suspension"] = True

    _update_endpoint_or_explain(
        name=LAKEBASE_ENDPOINT,
        endpoint=Endpoint(
            name=LAKEBASE_ENDPOINT,
            spec=EndpointSpec(**spec_kwargs),
        ),
        update_mask=FieldMask(field_mask=["spec"]),
    )

    elapsed = (time.time() - start) * 1000
    label = f"{timeout_seconds}s" if timeout_seconds > 0 else "disabled"
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Scale-to-zero timeout set to {label}",
    }


@app.post("/api/s2z/query")
async def s2z_query():
    """Run a test query and record the latency. On cold start, runs a second
    warm query to measure wake-up overhead separately."""
    import datetime

    # Get current state before query
    info = _get_endpoint_info()
    pre_state = info["state"]
    is_cold = pre_state == "SUSPENDED"

    # Run the query (includes wake-up time if suspended)
    sql = f"SELECT COUNT(*) AS cnt FROM {LAKEBASE_SCHEMA}.accounts"
    start = time.time()
    query_lakebase(sql)
    elapsed = (time.time() - start) * 1000

    # On cold start, immediately run a second warm query to isolate wake-up time
    warm_ms = None
    wakeup_ms = None
    if is_cold:
        start2 = time.time()
        query_lakebase(sql)
        warm_ms = round((time.time() - start2) * 1000, 2)
        wakeup_ms = round(elapsed - warm_ms, 2)

    entry = {
        "elapsed_ms": round(elapsed, 2),
        "pre_state": pre_state,
        "cold_start": is_cold,
        "warm_ms": warm_ms,
        "wakeup_ms": wakeup_ms,
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    _s2z_history.append(entry)

    return entry


@app.post("/api/s2z/clear-history")
async def s2z_clear_history():
    """Clear the query latency history."""
    _s2z_history.clear()
    return {"success": True}


# --- Row-Level Security Demo ---

RLS_TABLE = "advisors"
RLS_SCHEMA_TABLE = f"{LAKEBASE_SCHEMA}.{RLS_TABLE}"
RLS_PK = "advisor_id"
RLS_REGIONS = ["Hamilton", "Kingston", "London", "Mississauga", "Ottawa", "Sudbury", "Toronto", "Windsor"]
RLS_PERSONAS = {r: f"rls_{r.lower()}" for r in RLS_REGIONS}  # region -> pg role

# Track whether RLS has been set up on this app instance
_rls_state: dict[str, Any] = {"enabled": True}  # Pre-configured via setup script


def _query_lakebase_as_role(sql: str, role: str | None = None) -> tuple[list[str], list[dict]]:
    """Query Lakebase, optionally as a specific Postgres role."""
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            if role:
                cur.execute(f"SET ROLE {role}")
            else:
                cur.execute("RESET ROLE")
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = []
            for raw in cur.fetchall():
                row = {}
                for i, col in enumerate(columns):
                    val = raw[i]
                    if isinstance(val, Decimal):
                        val = float(val)
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    row[col] = val
                rows.append(row)
            return columns, rows
    finally:
        conn.close()


@app.get("/api/rls/status")
async def rls_status():
    """Get RLS status and available personas."""
    return {
        "enabled": _rls_state["enabled"],
        "table": RLS_SCHEMA_TABLE,
        "personas": [
            {"region": r, "role": RLS_PERSONAS[r]} for r in RLS_REGIONS
        ],
    }


@app.get("/api/rls/query")
async def rls_query(
    persona: str = Query("admin"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    """Query advisors table as a specific persona."""
    offset = (page - 1) * page_size
    role = None
    if persona != "admin" and persona in RLS_PERSONAS:
        role = RLS_PERSONAS[persona]

    count_sql = f"SELECT COUNT(*) AS cnt FROM {RLS_SCHEMA_TABLE}"
    data_sql = f"SELECT * FROM {RLS_SCHEMA_TABLE} ORDER BY {RLS_PK} LIMIT {page_size} OFFSET {offset}"

    start = time.time()
    _, count_rows = _query_lakebase_as_role(count_sql, role)
    columns, rows = _query_lakebase_as_role(data_sql, role)
    elapsed = (time.time() - start) * 1000

    return {
        "persona": persona,
        "role": role or "owner (bypasses RLS)",
        "columns": columns,
        "rows": rows,
        "total_count": count_rows[0]["cnt"],
        "page": page,
        "page_size": page_size,
        "elapsed_ms": round(elapsed, 2),
    }


# --- Point-in-Time Restore Demo ---

PITR_TABLE = "advisors"
PITR_SCHEMA_TABLE = f"{LAKEBASE_SCHEMA}.{PITR_TABLE}"
PITR_PK = "advisor_id"
PITR_BRANCH_ID = "restored"
PITR_BRANCH_NAME = f"projects/{LAKEBASE_PROJECT}/branches/{PITR_BRANCH_ID}"

_pitr_state: dict[str, Any] = {
    "checkpoint_ts": None,       # Postgres timestamp string before disaster
    "disaster_done": False,      # Whether destructive action has been applied
    "disaster_detail": None,     # Description of what was deleted
    "deleted_rows": [],          # Rows deleted (for cleanup re-insert)
    "restored_branch": False,    # Whether restored branch exists
    "restored_host": None,
    "restored_endpoint": None,
}


def _get_pitr_branch_connection():
    """Connect to the restored branch endpoint via psycopg."""
    import psycopg

    if not _pitr_state["restored_endpoint"]:
        raise HTTPException(400, "No restored branch. Run restore first.")

    cred = w.postgres.generate_database_credential(endpoint=_pitr_state["restored_endpoint"])
    me = w.current_user.me()
    username = me.user_name or me.display_name
    host = _pitr_state["restored_host"]
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host
    return psycopg.connect(
        host=host, hostaddr=ip, dbname=LAKEBASE_DB,
        user=username, password=cred.token, sslmode="require",
    )


def _query_pitr_branch(sql: str) -> tuple[list[str], list[dict]]:
    conn = _get_pitr_branch_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = []
            for raw in cur.fetchall():
                row = {}
                for i, col in enumerate(columns):
                    val = raw[i]
                    if isinstance(val, Decimal):
                        val = float(val)
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    row[col] = val
                rows.append(row)
            return columns, rows
    finally:
        conn.close()


def _discover_pitr_branch() -> bool:
    """Check if the restored branch exists and discover its endpoint."""
    if _pitr_state["restored_endpoint"]:
        return True
    try:
        w.postgres.get_branch(name=PITR_BRANCH_NAME)
    except Exception:
        return False
    endpoints = list(w.postgres.list_endpoints(parent=PITR_BRANCH_NAME))
    if endpoints and endpoints[0].status and endpoints[0].status.hosts:
        _pitr_state["restored_host"] = endpoints[0].status.hosts.host
        _pitr_state["restored_endpoint"] = endpoints[0].name
        _pitr_state["restored_branch"] = True
        return True
    return False


@app.get("/api/pitr/status")
async def pitr_status():
    """Get current PITR demo state."""
    # Get current row count from production
    prod_count = 0
    try:
        _, rows = query_lakebase(f"SELECT COUNT(*) AS cnt FROM {PITR_SCHEMA_TABLE}")
        prod_count = rows[0]["cnt"]
    except Exception:
        pass

    restored_count = 0
    has_branch = _discover_pitr_branch()
    if has_branch:
        try:
            _, rows = _query_pitr_branch(f"SELECT COUNT(*) AS cnt FROM {PITR_SCHEMA_TABLE}")
            restored_count = rows[0]["cnt"]
        except Exception:
            pass

    return {
        "checkpoint_ts": _pitr_state["checkpoint_ts"],
        "disaster_done": _pitr_state["disaster_done"],
        "disaster_detail": _pitr_state["disaster_detail"],
        "restored_branch": has_branch,
        "recovered": _pitr_state.get("recovered", False),
        "prod_count": prod_count,
        "restored_count": restored_count,
        "table": PITR_TABLE,
    }


@app.post("/api/pitr/disaster")
async def pitr_disaster(region: str = Query("Toronto")):
    """Simulate a disaster: capture timestamp, then corrupt advisor contact data by NULLing email and phone."""
    if _pitr_state["disaster_done"]:
        raise HTTPException(400, "Disaster already applied. Clean up or restore first.")

    start = time.time()

    # Automatically capture restore point BEFORE the corruption
    _, ts_rows = query_lakebase("SELECT NOW()::text AS ts")
    _pitr_state["checkpoint_ts"] = ts_rows[0]["ts"]

    # Save original values for cleanup
    _, originals = query_lakebase(
        f"SELECT advisor_id, email, phone FROM {PITR_SCHEMA_TABLE} WHERE LOWER(region) = LOWER('{region}')"
    )
    _pitr_state["deleted_rows"] = originals
    _pitr_state["disaster_region"] = region

    # Corrupt: NULL out email and phone
    affected = execute_lakebase(
        f"UPDATE {PITR_SCHEMA_TABLE} SET email = NULL, phone = NULL WHERE LOWER(region) = LOWER('{region}')"
    )

    _pitr_state["disaster_done"] = True
    _pitr_state["disaster_detail"] = f"Corrupted {affected} advisors in {region} (email & phone set to NULL)"

    elapsed = (time.time() - start) * 1000

    return {
        "success": True,
        "affected": affected,
        "region": region,
        "checkpoint_ts": _pitr_state["checkpoint_ts"],
        "elapsed_ms": round(elapsed, 2),
        "message": f"Corrupted {affected} advisors in {region} — email & phone wiped to NULL",
    }


@app.post("/api/pitr/restore")
async def pitr_restore():
    """Create a branch from the checkpoint timestamp to recover deleted data."""
    from databricks.sdk.service.postgres import Branch, BranchSpec, Endpoint, EndpointSpec, EndpointType
    from google.protobuf.timestamp_pb2 import Timestamp
    import datetime

    if not _pitr_state["disaster_done"] or not _pitr_state["checkpoint_ts"]:
        raise HTTPException(400, "No disaster to recover from. Simulate a disaster first.")

    # If branch already exists, clean it up first
    if _discover_pitr_branch():
        _pitr_state["restored_host"] = None
        _pitr_state["restored_endpoint"] = None
        _pitr_state["restored_branch"] = False
        try:
            w.postgres.delete_branch(name=PITR_BRANCH_NAME).wait()
        except Exception:
            pass

    start = time.time()

    # Parse the checkpoint timestamp and go back 30 minutes
    ts_str = _pitr_state["checkpoint_ts"]
    # Postgres NOW() returns e.g. "2026-03-09 14:30:00.123456+00"
    dt = datetime.datetime.fromisoformat(ts_str.replace("+00", "+00:00"))
    dt = dt - datetime.timedelta(minutes=5)
    _pitr_state["restore_ts"] = dt.isoformat()
    pb_ts = Timestamp()
    pb_ts.FromDatetime(dt)

    # Create branch from production at 5 minutes before the disaster
    w.postgres.create_branch(
        parent=f"projects/{LAKEBASE_PROJECT}",
        branch=Branch(
            spec=BranchSpec(
                source_branch=f"projects/{LAKEBASE_PROJECT}/branches/production",
                source_branch_time=pb_ts,
                no_expiry=True,
            )
        ),
        branch_id=PITR_BRANCH_ID,
    ).wait()

    # Create compute endpoint on the restored branch (skip if auto-created)
    try:
        w.postgres.create_endpoint(
            parent=PITR_BRANCH_NAME,
            endpoint=Endpoint(
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=1.0,
                    autoscaling_limit_max_cu=1.0,
                )
            ),
            endpoint_id="primary",
        ).wait()
    except Exception:
        pass  # Endpoint may have been auto-created with the branch

    # Discover the endpoint
    endpoints = list(w.postgres.list_endpoints(parent=PITR_BRANCH_NAME))
    if not endpoints or not endpoints[0].status or not endpoints[0].status.hosts:
        raise HTTPException(500, "Restored branch has no endpoint")
    _pitr_state["restored_host"] = endpoints[0].status.hosts.host
    _pitr_state["restored_endpoint"] = endpoints[0].name
    _pitr_state["restored_branch"] = True

    # Get row count from restored branch
    _, count_rows = _query_pitr_branch(f"SELECT COUNT(*) AS cnt FROM {PITR_SCHEMA_TABLE}")

    elapsed = (time.time() - start) * 1000
    restore_ts = _pitr_state["restore_ts"]
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "restored_count": count_rows[0]["cnt"],
        "checkpoint_ts": ts_str,
        "restore_ts": restore_ts,
        "message": f"Restored from 5 min before disaster ({restore_ts}) in {elapsed/1000:.1f}s — {count_rows[0]['cnt']} advisors recovered",
    }


@app.get("/api/pitr/compare")
async def pitr_compare(
    page: int = Query(1, ge=1),
    page_size: int = Query(15, ge=1, le=100),
):
    """Compare production (damaged) vs restored branch side by side."""
    offset = (page - 1) * page_size
    count_sql = f"SELECT COUNT(*) AS cnt FROM {PITR_SCHEMA_TABLE}"
    data_sql = f"SELECT * FROM {PITR_SCHEMA_TABLE} ORDER BY {PITR_PK} LIMIT {page_size} OFFSET {offset}"

    # Production
    start = time.time()
    _, prod_count = query_lakebase(count_sql)
    prod_cols, prod_rows = query_lakebase(data_sql)
    prod_ms = (time.time() - start) * 1000

    # Restored branch
    restored_cols, restored_rows, restored_total, restored_ms = [], [], 0, 0.0
    if _discover_pitr_branch():
        start = time.time()
        _, res_count = _query_pitr_branch(count_sql)
        restored_cols, restored_rows = _query_pitr_branch(data_sql)
        restored_total = res_count[0]["cnt"]
        restored_ms = (time.time() - start) * 1000

    return {
        "production": {
            "columns": prod_cols,
            "rows": prod_rows,
            "total_count": prod_count[0]["cnt"],
            "elapsed_ms": round(prod_ms, 2),
        },
        "restored": {
            "columns": restored_cols,
            "rows": restored_rows,
            "total_count": restored_total,
            "elapsed_ms": round(restored_ms, 2),
        },
        "page": page,
        "page_size": page_size,
    }


@app.post("/api/pitr/recover")
async def pitr_recover():
    """Copy intact data from restored branch back to production to fix corruption."""
    if not _discover_pitr_branch():
        raise HTTPException(400, "No restored branch. Run restore first.")

    region = _pitr_state.get("disaster_region")
    if not region:
        raise HTTPException(400, "No disaster region recorded.")

    start = time.time()

    # Read intact rows from the restored branch for the affected region
    _, intact_rows = _query_pitr_branch(
        f"SELECT advisor_id, email, phone FROM {PITR_SCHEMA_TABLE} WHERE LOWER(region) = LOWER('{region}')"
    )

    # Update production with the intact values
    conn = _get_pg_connection()
    updated = 0
    try:
        with conn.cursor() as cur:
            for row in intact_rows:
                cur.execute(
                    f"UPDATE {PITR_SCHEMA_TABLE} SET email = %s, phone = %s WHERE {PITR_PK} = %s",
                    (row["email"], row["phone"], row["advisor_id"]),
                )
                updated += 1
            conn.commit()
    finally:
        conn.close()

    _pitr_state["recovered"] = True
    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "updated": updated,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Recovered {updated} advisors in {region} — email & phone restored from backup branch",
    }


@app.post("/api/pitr/cleanup")
async def pitr_cleanup():
    """Clean up: restore production if needed and delete the restored branch."""
    start = time.time()
    messages = []

    # Restore original email/phone values in production (skip if already recovered)
    if _pitr_state["deleted_rows"] and not _pitr_state.get("recovered"):
        conn = _get_pg_connection()
        try:
            with conn.cursor() as cur:
                for row in _pitr_state["deleted_rows"]:
                    cur.execute(
                        f"UPDATE {PITR_SCHEMA_TABLE} SET email = %s, phone = %s WHERE {PITR_PK} = %s",
                        (row["email"], row["phone"], row["advisor_id"]),
                    )
                conn.commit()
            messages.append(f"Restored email & phone for {len(_pitr_state['deleted_rows'])} advisors in production")
        finally:
            conn.close()

    # Delete the restored branch
    if _pitr_state["restored_endpoint"] or _discover_pitr_branch():
        try:
            w.postgres.delete_branch(name=PITR_BRANCH_NAME).wait()
            messages.append("Deleted restored branch")
        except Exception as e:
            if "NOT_FOUND" not in str(e):
                messages.append(f"Warning: could not delete branch: {e}")

    # Reset state
    _pitr_state["checkpoint_ts"] = None
    _pitr_state["disaster_done"] = False
    _pitr_state["disaster_detail"] = None
    _pitr_state["disaster_region"] = None
    _pitr_state["deleted_rows"] = []
    _pitr_state["recovered"] = False
    _pitr_state["restored_branch"] = False
    _pitr_state["restored_host"] = None
    _pitr_state["restored_endpoint"] = None

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": "; ".join(messages) if messages else "Nothing to clean up",
    }


@app.post("/api/pitr/reset")
async def pitr_reset():
    """Force-reset the PITR demo: restore production data, delete branch, clear state. Always works."""
    start = time.time()
    messages = []

    # Restore original email/phone values in production (skip if already recovered)
    if _pitr_state["deleted_rows"] and not _pitr_state.get("recovered"):
        try:
            conn = _get_pg_connection()
            with conn.cursor() as cur:
                for row in _pitr_state["deleted_rows"]:
                    cur.execute(
                        f"UPDATE {PITR_SCHEMA_TABLE} SET email = %s, phone = %s WHERE {PITR_PK} = %s",
                        (row["email"], row["phone"], row["advisor_id"]),
                    )
                conn.commit()
            conn.close()
            messages.append(f"Restored {len(_pitr_state['deleted_rows'])} advisors")
        except Exception as e:
            messages.append(f"Warning: could not restore data: {e}")

    # Delete the restored branch if it exists
    try:
        w.postgres.get_branch(name=PITR_BRANCH_NAME)
        w.postgres.delete_branch(name=PITR_BRANCH_NAME).wait()
        messages.append("Deleted restored branch")
    except Exception:
        pass

    # Reset all state
    _pitr_state["checkpoint_ts"] = None
    _pitr_state["disaster_done"] = False
    _pitr_state["disaster_detail"] = None
    _pitr_state["disaster_region"] = None
    _pitr_state["deleted_rows"] = []
    _pitr_state["recovered"] = False
    _pitr_state["restored_branch"] = False
    _pitr_state["restored_host"] = None
    _pitr_state["restored_endpoint"] = None

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": "; ".join(messages) if messages else "Reset complete",
    }


# --- CI/CD Schema Migration Demo ---

CICD_TABLE = "advisors"
CICD_SCHEMA_TABLE = f"{LAKEBASE_SCHEMA}.{CICD_TABLE}"
CICD_ENVS = ["development", "staging", "production"]
CICD_BRANCH_IDS = {"development": "cicd-dev", "staging": "cicd-staging"}
CICD_BRANCH_NAMES = {
    env: f"projects/{LAKEBASE_PROJECT}/branches/{bid}"
    for env, bid in CICD_BRANCH_IDS.items()
}

# Migration changelog — each migration is applied in order
CICD_EXT_TABLE = f"{LAKEBASE_SCHEMA}.advisor_risk_scores"

CICD_MIGRATIONS = [
    {
        "id": "001",
        "description": "Create advisor_risk_scores table",
        "up": f"""CREATE TABLE IF NOT EXISTS {CICD_EXT_TABLE} (
            advisor_id INTEGER PRIMARY KEY,
            risk_score INTEGER DEFAULT 0,
            exam_priority TEXT DEFAULT 'Routine'
        )""",
        "down": f"DROP TABLE IF EXISTS {CICD_EXT_TABLE}",
    },
    {
        "id": "002",
        "description": "Register every advisor for risk scoring",
        "up": f"INSERT INTO {CICD_EXT_TABLE} (advisor_id) SELECT advisor_id FROM {CICD_SCHEMA_TABLE} ON CONFLICT DO NOTHING",
        "down": f"DELETE FROM {CICD_EXT_TABLE}",
    },
    {
        "id": "003",
        "description": "Score advisor risk (1-5) from complaint history",
        "up": f"UPDATE {CICD_EXT_TABLE} t SET risk_score = s.score FROM (SELECT a.advisor_id, NTILE(5) OVER (ORDER BY COUNT(c.complaint_id), a.advisor_id) AS score FROM {CICD_SCHEMA_TABLE} a LEFT JOIN {LAKEBASE_SCHEMA}.accounts ac ON ac.advisor_id = a.advisor_id LEFT JOIN {LAKEBASE_SCHEMA}.complaints c ON c.account_id = ac.account_id AND c.status <> 'DISMISSED' GROUP BY a.advisor_id) s WHERE t.advisor_id = s.advisor_id AND t.risk_score = 0",
        "down": f"UPDATE {CICD_EXT_TABLE} SET risk_score = 0",
    },
    {
        "id": "004",
        "description": "Set exam priority from risk score",
        "up": f"UPDATE {CICD_EXT_TABLE} SET exam_priority = CASE WHEN risk_score >= 5 THEN 'Priority Examination' WHEN risk_score >= 4 THEN 'Desk Review' ELSE 'Routine' END WHERE exam_priority = 'Routine' AND risk_score > 0",
        "down": f"UPDATE {CICD_EXT_TABLE} SET exam_priority = 'Routine'",
    },
]

# Track branch connections: {env: {"host": ..., "endpoint": ...}}
_cicd_state: dict[str, dict[str, Any]] = {
    "development": {"host": None, "endpoint": None},
    "staging": {"host": None, "endpoint": None},
}


def _get_cicd_connection(env: str):
    """Get a psycopg connection for the given environment."""
    import psycopg

    if env == "production":
        return _get_pg_connection()

    state = _cicd_state[env]
    if not state["endpoint"]:
        raise HTTPException(400, f"No {env} environment. Create it first.")

    cred = w.postgres.generate_database_credential(endpoint=state["endpoint"])
    me = w.current_user.me()
    username = me.user_name or me.display_name
    host = state["host"]
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host
    return psycopg.connect(
        host=host, hostaddr=ip, dbname=LAKEBASE_DB,
        user=username, password=cred.token, sslmode="require",
    )


def _cicd_query(env: str, sql: str) -> tuple[list[str], list[dict]]:
    conn = _get_cicd_connection(env)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = []
            for raw in cur.fetchall():
                row = {}
                for i, col in enumerate(columns):
                    val = raw[i]
                    if isinstance(val, Decimal):
                        val = float(val)
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    row[col] = val
                rows.append(row)
            return columns, rows
    finally:
        conn.close()


def _cicd_execute(env: str, sql: str) -> int:
    conn = _get_cicd_connection(env)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.rowcount
    finally:
        conn.close()


def _discover_cicd_branch(env: str) -> bool:
    """Check if a CI/CD branch exists and discover its endpoint."""
    if env == "production":
        return True
    state = _cicd_state[env]
    if state["endpoint"]:
        return True
    branch_name = CICD_BRANCH_NAMES[env]
    try:
        w.postgres.get_branch(name=branch_name)
    except Exception:
        return False
    endpoints = list(w.postgres.list_endpoints(parent=branch_name))
    if endpoints and endpoints[0].status and endpoints[0].status.hosts:
        state["host"] = endpoints[0].status.hosts.host
        state["endpoint"] = endpoints[0].name
        return True
    return False


def _ensure_migrations_table(env: str):
    """Create the schema_migrations tracking table if it doesn't exist."""
    _cicd_execute(env, f"""
        CREATE TABLE IF NOT EXISTS {LAKEBASE_SCHEMA}.schema_migrations (
            migration_id TEXT PRIMARY KEY,
            description TEXT,
            applied_at TIMESTAMP DEFAULT NOW()
        )
    """)


def _get_applied_migrations(env: str) -> list[str]:
    """Get list of applied migration IDs for an environment."""
    try:
        _ensure_migrations_table(env)
        _, rows = _cicd_query(env, f"SELECT migration_id FROM {LAKEBASE_SCHEMA}.schema_migrations ORDER BY migration_id")
        return [r["migration_id"] for r in rows]
    except Exception:
        return []


def _get_schema_columns(env: str) -> list[dict]:
    """Get column info for advisors + advisor_risk_scores (if it exists)."""
    results = []
    # Base advisors table
    _, base_rows = _cicd_query(env, f"""
        SELECT column_name, data_type, column_default
        FROM information_schema.columns
        WHERE table_schema = '{LAKEBASE_SCHEMA}' AND table_name = '{CICD_TABLE}'
        ORDER BY ordinal_position
    """)
    results.extend(base_rows)
    # Extension table (advisor_risk_scores) if migrations created it
    try:
        _, ext_rows = _cicd_query(env, f"""
            SELECT column_name, data_type, column_default
            FROM information_schema.columns
            WHERE table_schema = '{LAKEBASE_SCHEMA}' AND table_name = 'advisor_risk_scores'
            AND column_name != 'advisor_id'
            ORDER BY ordinal_position
        """)
        results.extend(ext_rows)
    except Exception:
        pass
    col_names = [r["column_name"] for r in results]
    print(f"[CICD] Schema columns for {env}: {col_names}")
    return results


def _cicd_data_query(env: str, limit: int, offset: int = 0) -> tuple[list[str], list[dict]]:
    """Query advisors with optional LEFT JOIN to advisor_risk_scores if it exists."""
    # Check if advisor_risk_scores table exists
    has_metrics = False
    try:
        _cicd_query(env, f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{LAKEBASE_SCHEMA}' AND table_name = 'advisor_risk_scores'")
        _, check = _cicd_query(env, f"SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema = '{LAKEBASE_SCHEMA}' AND table_name = 'advisor_risk_scores'")
        has_metrics = check and check[0]["cnt"] > 0
    except Exception:
        pass

    if has_metrics:
        sql = f"""
            SELECT a.*, m.risk_score, m.exam_priority
            FROM {CICD_SCHEMA_TABLE} a
            LEFT JOIN {CICD_EXT_TABLE} m ON a.advisor_id = m.advisor_id
            ORDER BY a.advisor_id LIMIT {limit} OFFSET {offset}
        """
    else:
        sql = f"SELECT * FROM {CICD_SCHEMA_TABLE} ORDER BY advisor_id LIMIT {limit} OFFSET {offset}"
    return _cicd_query(env, sql)


@app.get("/api/cicd/status")
async def cicd_status():
    """Get the state of all environments: branches, applied migrations, schemas."""
    envs = {}
    for env in CICD_ENVS:
        exists = _discover_cicd_branch(env)
        applied = []
        schema = []
        sample_rows: list[dict] = []
        error = None
        if exists:
            try:
                applied = _get_applied_migrations(env)
                schema = _get_schema_columns(env)
                _, sample_rows = _cicd_data_query(env, limit=5)
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                print(f"[CICD] Error fetching {env} status: {error}")
        envs[env] = {
            "exists": exists,
            "applied_migrations": applied,
            "schema": schema,
            "sample_rows": sample_rows,
            "pending_count": len([m for m in CICD_MIGRATIONS if m["id"] not in applied]) if exists else 0,
            "error": error,
        }

    return {
        "environments": envs,
        "migrations": [{"id": m["id"], "description": m["description"]} for m in CICD_MIGRATIONS],
    }


@app.post("/api/cicd/create-env")
async def cicd_create_env(env: str = Query(...)):
    """Create a branch for dev or staging environment."""
    if env not in CICD_BRANCH_IDS:
        raise HTTPException(400, f"Cannot create {env} — only development and staging")
    if _discover_cicd_branch(env):
        # Branch already exists — clean up any stale inherited migration records
        try:
            _ensure_migrations_table(env)
            inherited = _get_applied_migrations(env)
            if inherited:
                schema_cols = [r["column_name"] for r in _get_schema_columns(env)]
                expected_cols = ["risk_score", "exam_priority"]
                if not any(c in schema_cols for c in expected_cols):
                    print(f"[CICD] Existing branch {env} has stale migrations, cleaning up")
                    for m in reversed(CICD_MIGRATIONS):
                        if m["id"] in inherited:
                            try:
                                _cicd_execute(env, m["down"])
                            except Exception:
                                pass
                    _cicd_execute(env, f"DELETE FROM {LAKEBASE_SCHEMA}.schema_migrations")
        except Exception as e:
            print(f"[CICD] Error cleaning up existing branch {env}: {e}")
        return {"status": "exists", "env": env}

    from databricks.sdk.service.postgres import Branch, BranchSpec, Endpoint, EndpointSpec, EndpointType

    start = time.time()
    branch_id = CICD_BRANCH_IDS[env]
    branch_name = CICD_BRANCH_NAMES[env]

    w.postgres.create_branch(
        parent=f"projects/{LAKEBASE_PROJECT}",
        branch=Branch(
            spec=BranchSpec(
                source_branch=f"projects/{LAKEBASE_PROJECT}/branches/production",
                no_expiry=True,
            )
        ),
        branch_id=branch_id,
    ).wait()

    # Create endpoint if not auto-created
    try:
        w.postgres.create_endpoint(
            parent=branch_name,
            endpoint=Endpoint(
                spec=EndpointSpec(
                    endpoint_type=EndpointType.ENDPOINT_TYPE_READ_WRITE,
                    autoscaling_limit_min_cu=1.0,
                    autoscaling_limit_max_cu=1.0,
                )
            ),
            endpoint_id="primary",
        ).wait()
    except Exception:
        pass

    # Discover endpoint
    endpoints = list(w.postgres.list_endpoints(parent=branch_name))
    if endpoints and endpoints[0].status and endpoints[0].status.hosts:
        _cicd_state[env]["host"] = endpoints[0].status.hosts.host
        _cicd_state[env]["endpoint"] = endpoints[0].name

    # Rollback any inherited migrations so the branch starts clean
    _ensure_migrations_table(env)
    inherited = _get_applied_migrations(env)
    if inherited:
        for m in reversed(CICD_MIGRATIONS):
            if m["id"] in inherited:
                try:
                    _cicd_execute(env, m["down"])
                except Exception:
                    pass
        _cicd_execute(env, f"DELETE FROM {LAKEBASE_SCHEMA}.schema_migrations")

    elapsed = (time.time() - start) * 1000
    return {
        "status": "created",
        "env": env,
        "elapsed_ms": round(elapsed, 2),
        "message": f"{env.title()} environment created in {elapsed/1000:.1f}s",
    }


@app.post("/api/cicd/migrate")
async def cicd_migrate(env: str = Query(...)):
    """Apply all pending migrations to the given environment."""
    if env not in CICD_ENVS:
        raise HTTPException(400, f"Unknown environment: {env}")
    if not _discover_cicd_branch(env):
        raise HTTPException(400, f"{env.title()} environment does not exist")

    start = time.time()
    _ensure_migrations_table(env)

    applied = _get_applied_migrations(env)

    # Detect stale migration records: schema_migrations says columns exist but they don't
    if applied and env != "production":
        schema_cols = [r["column_name"] for r in _get_schema_columns(env)]
        expected_cols = ["risk_score", "exam_priority"]
        if not any(c in schema_cols for c in expected_cols):
            print(f"[CICD] Stale migration records on {env}: {applied} but columns {expected_cols} missing from {schema_cols}")
            _cicd_execute(env, f"DELETE FROM {LAKEBASE_SCHEMA}.schema_migrations")
            applied = []

    pending = [m for m in CICD_MIGRATIONS if m["id"] not in applied]

    if not pending:
        return {"env": env, "applied": 0, "message": "All migrations already applied"}

    results = []
    for m in pending:
        try:
            print(f"[CICD] Applying migration {m['id']} to {env}: {m['description']}")
            print(f"[CICD]   SQL: {m['up']}")
            affected = _cicd_execute(env, m["up"])
            print(f"[CICD]   Affected rows: {affected}")
            _cicd_execute(env, f"""
                INSERT INTO {LAKEBASE_SCHEMA}.schema_migrations (migration_id, description)
                VALUES ('{m["id"]}', '{m["description"]}')
                ON CONFLICT (migration_id) DO NOTHING
            """)
            results.append({"id": m["id"], "status": "applied", "affected": affected})
        except Exception as e:
            print(f"[CICD] Migration {m['id']} FAILED: {e}")
            results.append({"id": m["id"], "status": "failed", "error": str(e)})
            break

    elapsed = (time.time() - start) * 1000
    applied_count = len([r for r in results if r["status"] == "applied"])
    return {
        "env": env,
        "applied": applied_count,
        "results": results,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Applied {applied_count} migration(s) to {env}",
    }


@app.post("/api/cicd/rollback")
async def cicd_rollback(env: str = Query(...)):
    """Rollback all applied migrations on the given environment (in reverse order)."""
    if env not in CICD_ENVS:
        raise HTTPException(400, f"Unknown environment: {env}")
    if not _discover_cicd_branch(env):
        raise HTTPException(400, f"{env.title()} environment does not exist")

    start = time.time()
    applied = _get_applied_migrations(env)

    if not applied:
        return {"env": env, "rolled_back": 0, "message": "No migrations to roll back"}

    # Rollback in reverse order
    rolled_back = 0
    for m in reversed(CICD_MIGRATIONS):
        if m["id"] in applied:
            try:
                _cicd_execute(env, m["down"])
                _cicd_execute(env, f"DELETE FROM {LAKEBASE_SCHEMA}.schema_migrations WHERE migration_id = '{m['id']}'")
                rolled_back += 1
            except Exception:
                break

    elapsed = (time.time() - start) * 1000
    return {
        "env": env,
        "rolled_back": rolled_back,
        "elapsed_ms": round(elapsed, 2),
        "message": f"Rolled back {rolled_back} migration(s) on {env}",
    }


@app.get("/api/cicd/compare")
async def cicd_compare(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
):
    """Compare data across all active environments."""
    offset = (page - 1) * page_size
    result = {}

    for env in CICD_ENVS:
        if not _discover_cicd_branch(env):
            result[env] = {"columns": [], "rows": [], "total_count": 0, "elapsed_ms": 0}
            continue
        try:
            start = time.time()
            _, count_rows = _cicd_query(env, f"SELECT COUNT(*) AS cnt FROM {CICD_SCHEMA_TABLE}")
            cols, rows = _cicd_data_query(env, limit=page_size, offset=offset)
            elapsed = (time.time() - start) * 1000
            result[env] = {
                "columns": cols,
                "rows": rows,
                "total_count": count_rows[0]["cnt"],
                "elapsed_ms": round(elapsed, 2),
            }
        except Exception as e:
            result[env] = {"columns": [], "rows": [], "total_count": 0, "elapsed_ms": 0, "error": str(e)}

    return {"environments": result, "page": page, "page_size": page_size}


@app.post("/api/cicd/reset")
async def cicd_reset():
    """Delete dev and staging branches, rollback production migrations. Full reset."""
    start = time.time()
    messages = []

    # Rollback production migrations
    applied = _get_applied_migrations("production")
    if applied:
        for m in reversed(CICD_MIGRATIONS):
            if m["id"] in applied:
                try:
                    _cicd_execute("production", m["down"])
                    _cicd_execute("production", f"DELETE FROM {LAKEBASE_SCHEMA}.schema_migrations WHERE migration_id = '{m['id']}'")
                except Exception:
                    pass
        messages.append("Rolled back production migrations")

    # Delete dev and staging branches
    for env in ["development", "staging"]:
        branch_name = CICD_BRANCH_NAMES[env]
        try:
            w.postgres.get_branch(name=branch_name)
            w.postgres.delete_branch(name=branch_name).wait()
            messages.append(f"Deleted {env} branch")
        except Exception:
            pass
        _cicd_state[env] = {"host": None, "endpoint": None}

    elapsed = (time.time() - start) * 1000
    return {
        "success": True,
        "elapsed_ms": round(elapsed, 2),
        "message": "; ".join(messages) if messages else "Nothing to reset",
    }


# ============================================================
# ORM Demo — SQLAlchemy + Lakebase
# ============================================================

_sa_engine = None


def _get_sa_engine():
    """Create or return a SQLAlchemy engine connected to Lakebase."""
    global _sa_engine
    from sqlalchemy import create_engine, event

    cred = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
    me = w.current_user.me()
    username = me.user_name or me.display_name
    host = LAKEBASE_HOST
    try:
        ip = socket.gethostbyname(host)
    except Exception:
        ip = host

    url = f"postgresql+psycopg://{username}:{cred.token}@{host}/{LAKEBASE_DB}?sslmode=require"
    engine = create_engine(url, connect_args={"hostaddr": ip}, pool_pre_ping=True, pool_size=1, max_overflow=0)
    # Token refresh: replace password on every new connection
    @event.listens_for(engine, "do_connect")
    def on_connect(dialect, conn_rec, cargs, cparams):
        fresh = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
        cparams["password"] = fresh.token
    _sa_engine = engine
    return engine


def _get_sa_metadata():
    """Reflect all tables from the lakebase_demo schema."""
    from sqlalchemy import MetaData
    engine = _get_sa_engine()
    metadata = MetaData(schema=LAKEBASE_SCHEMA)
    metadata.reflect(bind=engine)
    return metadata, engine


ORM_TABLES = ["advisors", "accounts", "investors", "complaints", "contributions"]
ORM_RELATIONSHIPS = {
    "advisors": {"children": [("accounts", "advisor_id")]},
    "investors": {"children": [("accounts", "investor_id")]},
    "accounts": {"children": [("complaints", "account_id"), ("contributions", "account_id")]},
}

ORM_QUERIES = [
    {
        "id": "count_all",
        "name": "Count all advisors",
        "description": "Simple COUNT(*) query",
        "raw_sql": f"SELECT COUNT(*) AS total FROM {LAKEBASE_SCHEMA}.advisors",
        "orm_code": 'session.query(func.count(Advisors.c.advisor_id)).scalar()',
    },
    {
        "id": "filter_region",
        "name": "Advisors by region",
        "description": "Filter with WHERE clause",
        "raw_sql": f"SELECT * FROM {LAKEBASE_SCHEMA}.advisors WHERE region = 'Toronto' ORDER BY advisor_id LIMIT 10",
        "orm_code": 'session.query(Advisors).filter(Advisors.c.region == "Toronto").order_by(Advisors.c.advisor_id).limit(10).all()',
    },
    {
        "id": "join_accounts",
        "name": "Advisors with account count",
        "description": "JOIN + GROUP BY aggregation",
        "raw_sql": f"SELECT a.advisor_id, a.first_name, a.last_name, COUNT(p.account_id) AS account_count FROM {LAKEBASE_SCHEMA}.advisors a JOIN {LAKEBASE_SCHEMA}.accounts p ON a.advisor_id = p.advisor_id GROUP BY a.advisor_id, a.first_name, a.last_name ORDER BY account_count DESC LIMIT 10",
        "orm_code": 'session.query(Advisors.c.advisor_id, Advisors.c.first_name, Advisors.c.last_name, func.count(Accounts.c.account_id).label("account_count")).join(Accounts, Advisors.c.advisor_id == Accounts.c.advisor_id).group_by(Advisors.c.advisor_id, Advisors.c.first_name, Advisors.c.last_name).order_by(desc("account_count")).limit(10).all()',
    },
    {
        "id": "subquery_high_value",
        "name": "High-value accounts",
        "description": "Subquery with contribution aggregation",
        "raw_sql": f"SELECT p.account_id, p.status, pt.type_name, SUM(pr.amount) AS assets_under_admin FROM {LAKEBASE_SCHEMA}.accounts p JOIN {LAKEBASE_SCHEMA}.account_types pt ON p.account_type_id = pt.account_type_id JOIN {LAKEBASE_SCHEMA}.contributions pr ON p.account_id = pr.account_id GROUP BY p.account_id, p.status, pt.type_name HAVING SUM(pr.amount) > 900 ORDER BY assets_under_admin DESC LIMIT 10",
        "orm_code": 'select(Accounts.c.account_id, Accounts.c.status, AccountTypes.c.type_name, func.sum(Contributions.c.amount).label("assets_under_admin")).join(AccountTypes, ...).join(Contributions, ...).group_by(...).having(func.sum(Contributions.c.amount) > 900).order_by(desc("assets_under_admin")).limit(10)',
    },
    {
        "id": "complaint_stats",
        "name": "Complaints by status",
        "description": "Aggregation with CASE expression",
        "raw_sql": f"SELECT status, COUNT(*) AS complaint_count, ROUND(AVG(amount_claimed)::numeric, 2) AS avg_amount FROM {LAKEBASE_SCHEMA}.complaints GROUP BY status ORDER BY complaint_count DESC",
        "orm_code": 'select(Complaints.c.status, func.count().label("complaint_count"), func.round(func.avg(Complaints.c.amount_claimed), 2).label("avg_amount")).group_by(Complaints.c.status).order_by(desc("complaint_count"))',
    },
]


@app.get("/api/orm/tables")
async def orm_tables():
    """List available tables with column info via SQLAlchemy reflection."""
    start = time.time()
    try:
        metadata, engine = _get_sa_metadata()
        tables = []
        for tname in TABLES:
            key = f"{LAKEBASE_SCHEMA}.{tname}"
            if key in metadata.tables:
                t = metadata.tables[key]
                cols = [{"name": c.name, "type": str(c.type), "primary_key": c.primary_key, "nullable": c.nullable} for c in t.columns]
                fks = [{"column": list(fk.columns)[0].name, "references": f"{fk.referred_table.name}.{list(fk.elements)[0].column.name}"} for fk in t.foreign_key_constraints]
                tables.append({"name": tname, "columns": cols, "foreign_keys": fks, "column_count": len(cols), "fk_count": len(fks)})
        elapsed = (time.time() - start) * 1000
        return {"tables": tables, "elapsed_ms": round(elapsed, 2), "engine": str(engine.url).split("@")[0] + "@..."}
    except Exception as e:
        raise HTTPException(500, f"Reflection failed: {e}")


@app.get("/api/orm/query")
async def orm_query(query_id: str = Query(...)):
    """Run a predefined query using both raw SQL and SQLAlchemy ORM, return comparison."""
    q = next((q for q in ORM_QUERIES if q["id"] == query_id), None)
    if not q:
        raise HTTPException(400, f"Unknown query: {query_id}")

    from sqlalchemy import text, func, desc, MetaData
    from sqlalchemy.orm import Session

    metadata, engine = _get_sa_metadata()

    # Raw SQL via psycopg
    raw_start = time.time()
    raw_error = None
    try:
        cols, rows = query_lakebase(q["raw_sql"])
        raw_elapsed = (time.time() - raw_start) * 1000
    except Exception as e:
        cols, rows = [], []
        raw_elapsed = (time.time() - raw_start) * 1000
        raw_error = str(e)
        print(f"[ORM] Raw SQL error for {query_id}: {e}")

    # ORM query via SQLAlchemy
    orm_start = time.time()
    orm_error = None
    try:
        with Session(engine) as session:
            result = session.execute(text(q["raw_sql"]))
            orm_cols = list(result.keys())
            orm_rows = []
            for raw in result.fetchall():
                row = {}
                for i, col in enumerate(orm_cols):
                    val = raw[i]
                    if isinstance(val, Decimal):
                        val = float(val)
                    if hasattr(val, "isoformat"):
                        val = val.isoformat()
                    row[col] = val
                orm_rows.append(row)
        orm_elapsed = (time.time() - orm_start) * 1000
    except Exception as e:
        orm_cols, orm_rows = [], []
        orm_elapsed = (time.time() - orm_start) * 1000
        orm_error = str(e)
        print(f"[ORM] ORM error for {query_id}: {e}")

    return {
        "query": q,
        "raw_sql": {"columns": cols, "rows": rows, "elapsed_ms": round(raw_elapsed, 2), "error": raw_error},
        "orm": {"columns": orm_cols, "rows": orm_rows, "elapsed_ms": round(orm_elapsed, 2), "error": orm_error},
    }


@app.get("/api/orm/browse")
async def orm_browse(
    table: str = Query(...),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=50),
    filter_col: str = Query(None),
    filter_val: str = Query(None),
    order_by: str = Query(None),
    order_dir: str = Query("asc"),
):
    """Browse a table using SQLAlchemy ORM with optional filtering and sorting."""
    if table not in TABLES:
        raise HTTPException(400, f"Invalid table: {table}")

    from sqlalchemy import MetaData, select, func, String
    from sqlalchemy.orm import Session

    start = time.time()
    metadata, engine = _get_sa_metadata()
    key = f"{LAKEBASE_SCHEMA}.{table}"
    if key not in metadata.tables:
        raise HTTPException(400, f"Table {table} not found in schema")

    t = metadata.tables[key]
    pk_col = [c.name for c in t.primary_key.columns][0] if t.primary_key else t.columns.keys()[0]

    with Session(engine) as session:
        # Count
        count_q = select(func.count()).select_from(t)
        if filter_col and filter_val and filter_col in t.columns:
            count_q = count_q.where(t.c[filter_col].cast(String).ilike(f"%{filter_val}%"))
        total = session.execute(count_q).scalar()

        # Data
        sort_col = order_by if order_by and order_by in t.columns else pk_col
        data_q = select(t)
        if filter_col and filter_val and filter_col in t.columns:
            data_q = data_q.where(t.c[filter_col].cast(String).ilike(f"%{filter_val}%"))
        if order_dir == "desc":
            data_q = data_q.order_by(t.c[sort_col].desc())
        else:
            data_q = data_q.order_by(t.c[sort_col].asc())
        data_q = data_q.limit(page_size).offset((page - 1) * page_size)

        result = session.execute(data_q)
        columns = list(result.keys())
        rows = []
        for raw in result.fetchall():
            row = {}
            for i, col in enumerate(columns):
                val = raw[i]
                if isinstance(val, Decimal):
                    val = float(val)
                if hasattr(val, "isoformat"):
                    val = val.isoformat()
                row[col] = val
            rows.append(row)

    elapsed = (time.time() - start) * 1000

    # Build ORM code representation
    orm_code = f"select({table})"
    if filter_col and filter_val:
        orm_code += f'.where({table}.c.{filter_col}.ilike("%{filter_val}%"))'
    orm_code += f".order_by({table}.c.{sort_col}{'desc()' if order_dir == 'desc' else ''}).limit({page_size}).offset({(page-1)*page_size})"

    return {
        "table": table,
        "columns": columns,
        "rows": rows,
        "total_count": total,
        "page": page,
        "page_size": page_size,
        "elapsed_ms": round(elapsed, 2),
        "orm_code": orm_code,
        "pk_column": pk_col,
    }


@app.get("/api/orm/relationships")
async def orm_relationships(table: str = Query(...), pk_value: int = Query(...)):
    """Navigate relationships from a specific row using SQLAlchemy reflection."""
    if table not in TABLES:
        raise HTTPException(400, f"Invalid table: {table}")

    from sqlalchemy import select, func
    from sqlalchemy.orm import Session

    start = time.time()
    metadata, engine = _get_sa_metadata()
    key = f"{LAKEBASE_SCHEMA}.{table}"
    if key not in metadata.tables:
        raise HTTPException(400, f"Table {table} not found")

    t = metadata.tables[key]
    pk_col = [c.name for c in t.primary_key.columns][0] if t.primary_key else t.columns.keys()[0]

    with Session(engine) as session:
        # Get the source row
        source_q = select(t).where(t.c[pk_col] == pk_value)
        result = session.execute(source_q)
        source_cols = list(result.keys())
        source_raw = result.fetchone()
        if not source_raw:
            raise HTTPException(404, "Row not found")
        source_row = {}
        for i, col in enumerate(source_cols):
            val = source_raw[i]
            if isinstance(val, Decimal):
                val = float(val)
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            source_row[col] = val

        # Find related tables via foreign keys
        related = []
        for tname in TABLES:
            rkey = f"{LAKEBASE_SCHEMA}.{tname}"
            if rkey not in metadata.tables or tname == table:
                continue
            rt = metadata.tables[rkey]
            for fk in rt.foreign_key_constraints:
                referred = fk.referred_table.name
                if referred == table:
                    fk_col = list(fk.columns)[0].name
                    count_q = select(func.count()).select_from(rt).where(rt.c[fk_col] == pk_value)
                    cnt = session.execute(count_q).scalar()
                    sample_q = select(rt).where(rt.c[fk_col] == pk_value).limit(5)
                    sample_result = session.execute(sample_q)
                    sample_cols = list(sample_result.keys())
                    sample_rows = []
                    for raw in sample_result.fetchall():
                        row = {}
                        for i, col in enumerate(sample_cols):
                            val = raw[i]
                            if isinstance(val, Decimal):
                                val = float(val)
                            if hasattr(val, "isoformat"):
                                val = val.isoformat()
                            row[col] = val
                        sample_rows.append(row)
                    related.append({
                        "table": tname,
                        "fk_column": fk_col,
                        "count": cnt,
                        "columns": sample_cols,
                        "sample_rows": sample_rows,
                    })

    elapsed = (time.time() - start) * 1000
    return {
        "source_table": table,
        "source_row": source_row,
        "source_columns": source_cols,
        "related": related,
        "elapsed_ms": round(elapsed, 2),
    }


@app.get("/api/orm/queries")
async def orm_query_list():
    """List available predefined queries."""
    return {"queries": ORM_QUERIES}


# ============================================================
# Autoscaling Under Load Demo
# ============================================================

import asyncio
import concurrent.futures
import threading

_load_test_state: dict[str, Any] = {
    "running": False,
    "results": [],       # list of {query_id, query_type, started_at, elapsed_ms, success, error}
    "start_time": None,
    "concurrency": 10,
    "total_planned": 0,
    "total_done": 0,
    "stop_requested": False,
}
_load_test_lock = threading.Lock()

LOAD_QUERIES = {
    "heavy": f"""
        SELECT a.advisor_id, a.first_name, a.last_name, a.region,
               COUNT(DISTINCT p.account_id) AS accounts,
               COUNT(DISTINCT c.complaint_id) AS complaints,
               COALESCE(SUM(pr.amount), 0) AS total_contributions
        FROM {LAKEBASE_SCHEMA}.advisors a
        JOIN {LAKEBASE_SCHEMA}.accounts p ON a.advisor_id = p.advisor_id
        LEFT JOIN {LAKEBASE_SCHEMA}.complaints c ON p.account_id = c.account_id
        LEFT JOIN {LAKEBASE_SCHEMA}.contributions pr ON p.account_id = pr.account_id
        GROUP BY a.advisor_id, a.first_name, a.last_name, a.region
        ORDER BY total_contributions DESC
        LIMIT 50
    """,
    "medium": f"""
        SELECT p.account_id, p.status, p.market_value,
               COUNT(c.complaint_id) AS complaint_count,
               COALESCE(SUM(c.amount_claimed), 0) AS total_complaints
        FROM {LAKEBASE_SCHEMA}.accounts p
        LEFT JOIN {LAKEBASE_SCHEMA}.complaints c ON p.account_id = c.account_id
        GROUP BY p.account_id, p.status, p.market_value
        HAVING COUNT(c.complaint_id) > 0
        ORDER BY total_complaints DESC
        LIMIT 100
    """,
    "light": f"""
        SELECT region, COUNT(*) AS advisor_count,
               SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_count
        FROM {LAKEBASE_SCHEMA}.advisors
        GROUP BY region
        ORDER BY advisor_count DESC
    """,
}


def _run_single_query(query_id: int, query_type: str):
    """Run a single query and record the result."""
    import psycopg
    start = time.time()
    error = None
    success = True
    try:
        cred = w.postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
        me = w.current_user.me()
        username = me.user_name or me.display_name
        host = LAKEBASE_HOST
        try:
            ip = socket.gethostbyname(host)
        except Exception:
            ip = host
        conn = psycopg.connect(
            host=host, hostaddr=ip, dbname=LAKEBASE_DB,
            user=username, password=cred.token, sslmode="require",
            connect_timeout=30,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(LOAD_QUERIES[query_type])
                cur.fetchall()
        finally:
            conn.close()
    except Exception as e:
        success = False
        error = str(e)

    elapsed = (time.time() - start) * 1000
    result = {
        "query_id": query_id,
        "query_type": query_type,
        "elapsed_ms": round(elapsed, 2),
        "success": success,
        "error": error,
        "timestamp": round(time.time() - (_load_test_state["start_time"] or time.time()), 2),
    }
    with _load_test_lock:
        _load_test_state["results"].append(result)
        _load_test_state["total_done"] += 1


def _load_test_worker(concurrency: int, waves: int, mix: list[str]):
    """Background worker that fires waves of concurrent queries."""
    _load_test_state["start_time"] = time.time()
    query_id = 0

    for wave in range(waves):
        if _load_test_state["stop_requested"]:
            break

        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
            for i in range(concurrency):
                if _load_test_state["stop_requested"]:
                    break
                query_type = mix[query_id % len(mix)]
                futures.append(pool.submit(_run_single_query, query_id, query_type))
                query_id += 1
            concurrent.futures.wait(futures)

        # Brief pause between waves
        if not _load_test_state["stop_requested"] and wave < waves - 1:
            time.sleep(1)

    with _load_test_lock:
        _load_test_state["running"] = False


@app.post("/api/autoscale/configure")
async def autoscale_configure(min_cu: float = Query(1.0), max_cu: float = Query(8.0)):
    """Set the autoscaling CU range for the production endpoint."""
    from databricks.sdk.service.postgres import Endpoint, EndpointSpec, EndpointType, FieldMask

    start = time.time()
    # Get current config to preserve other settings
    current = _get_endpoint_info()
    spec_kwargs: dict[str, Any] = {
        "endpoint_type": EndpointType.ENDPOINT_TYPE_READ_WRITE,
        "autoscaling_limit_min_cu": min_cu,
        "autoscaling_limit_max_cu": max_cu,
    }
    # Preserve suspend timeout if set
    if current.get("suspend_timeout_seconds", 0) > 0:
        from google.protobuf.duration_pb2 import Duration
        spec_kwargs["suspend_timeout_duration"] = Duration(seconds=current["suspend_timeout_seconds"])
    else:
        spec_kwargs["no_suspension"] = True

    _update_endpoint_or_explain(
        name=LAKEBASE_ENDPOINT,
        endpoint=Endpoint(
            name=LAKEBASE_ENDPOINT,
            spec=EndpointSpec(**spec_kwargs),
        ),
        update_mask=FieldMask(field_mask=["spec"]),
    )
    elapsed = (time.time() - start) * 1000
    return {"min_cu": min_cu, "max_cu": max_cu, "elapsed_ms": round(elapsed, 2)}


@app.get("/api/autoscale/endpoint-status")
async def autoscale_endpoint_status():
    """Get current endpoint compute status."""
    info = _get_endpoint_info()
    return {
        "state": info["state"],
        "min_cu": info["min_cu"],
        "max_cu": info["max_cu"],
    }


@app.post("/api/autoscale/start")
async def autoscale_start(
    concurrency: int = Query(10, ge=1, le=50),
    waves: int = Query(5, ge=1, le=20),
    mix: str = Query("heavy,medium,light"),
):
    """Start a load test with concurrent query waves."""
    if _load_test_state["running"]:
        raise HTTPException(400, "Load test already running")

    query_mix = [m.strip() for m in mix.split(",") if m.strip() in LOAD_QUERIES]
    if not query_mix:
        query_mix = ["heavy", "medium", "light"]

    total = concurrency * waves
    with _load_test_lock:
        _load_test_state["running"] = True
        _load_test_state["results"] = []
        _load_test_state["start_time"] = None
        _load_test_state["concurrency"] = concurrency
        _load_test_state["total_planned"] = total
        _load_test_state["total_done"] = 0
        _load_test_state["stop_requested"] = False

    thread = threading.Thread(target=_load_test_worker, args=(concurrency, waves, query_mix), daemon=True)
    thread.start()

    return {"status": "started", "concurrency": concurrency, "waves": waves, "total_queries": total, "mix": query_mix}


@app.post("/api/autoscale/stop")
async def autoscale_stop():
    """Stop the running load test."""
    _load_test_state["stop_requested"] = True
    return {"status": "stop_requested"}


@app.get("/api/autoscale/results")
async def autoscale_results(since: int = Query(0)):
    """Get load test results. Pass since=N to get results after index N."""
    with _load_test_lock:
        results = _load_test_state["results"][since:]
        return {
            "running": _load_test_state["running"],
            "total_planned": _load_test_state["total_planned"],
            "total_done": _load_test_state["total_done"],
            "results": results,
            "since": since,
            "next_since": since + len(results),
        }


@app.post("/api/autoscale/baseline")
async def autoscale_baseline():
    """Run one query of each type to establish baseline latencies."""
    baselines = {}
    for qtype, sql in LOAD_QUERIES.items():
        start = time.time()
        try:
            query_lakebase(sql)
            elapsed = (time.time() - start) * 1000
            baselines[qtype] = {"elapsed_ms": round(elapsed, 2), "success": True}
        except Exception as e:
            elapsed = (time.time() - start) * 1000
            baselines[qtype] = {"elapsed_ms": round(elapsed, 2), "success": False, "error": str(e)}
    return {"baselines": baselines}


from dab_demo import router as dab_router  # noqa: E402

app.include_router(dab_router)

# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def home():
    return FileResponse("static/home.html")


@app.get("/lakebase-as-code")
async def lakebase_as_code():
    return FileResponse("static/lakebase-as-code.html")


@app.get("/dbsql-vs-lakebase")
async def dbsql_vs_lakebase():
    return FileResponse("static/index.html")


@app.get("/branching")
async def branching():
    return FileResponse("static/branching.html")


@app.get("/reverse-etl")
async def reverse_etl():
    return FileResponse("static/reverse-etl.html")


@app.get("/scale-to-zero")
async def scale_to_zero():
    return FileResponse("static/scale-to-zero.html")


@app.get("/row-level-security")
async def row_level_security():
    return FileResponse("static/row-level-security.html")


@app.get("/point-in-time-restore")
async def point_in_time_restore():
    return FileResponse("static/point-in-time-restore.html")


@app.get("/cicd")
async def cicd():
    return FileResponse("static/cicd.html")


@app.get("/orm")
async def orm():
    return FileResponse("static/orm.html")


@app.get("/autoscale")
async def autoscale():
    return FileResponse("static/autoscale.html")

