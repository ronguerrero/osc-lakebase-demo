"""OSC registrant check-in: a minimal Databricks App on Lakebase, deployed from a bundle."""
import html
import os

import psycopg
from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI()
w = WorkspaceClient()


def connect():
    cred = w.postgres.generate_database_credential(endpoint=os.environ["LAKEBASE_ENDPOINT"])
    conn = psycopg.connect(host=os.environ["PGHOST"], dbname=os.environ["PGDATABASE"],
                           user=os.environ["PGUSER"], password=cred.token, sslmode="require")
    # The app's role can create schemas (CAN_CONNECT_AND_CREATE) but not tables in public: use its own schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS osc_checkin")
    conn.execute("SET search_path TO osc_checkin")
    conn.execute("""CREATE TABLE IF NOT EXISTS registrant_checkins (
        id serial PRIMARY KEY,
        registrant text NOT NULL,
        district text NOT NULL,
        checked_in_by text,
        checked_in_at timestamptz DEFAULT now())""")
    return conn


@app.get("/health")
def health():
    with connect() as conn:
        total = conn.execute("SELECT count(*) FROM registrant_checkins").fetchone()[0]
    return {"status": "ok", "endpoint": os.environ["LAKEBASE_ENDPOINT"], "checkins": total}


@app.post("/checkin")
def checkin(request: Request, registrant: str = Form(...), district: str = Form(...)):
    with connect() as conn:
        conn.execute("INSERT INTO registrant_checkins (registrant, district, checked_in_by) VALUES (%s, %s, %s)",
                     (registrant, district, request.headers.get("x-forwarded-email")))
    return RedirectResponse("/", status_code=303)


@app.get("/", response_class=HTMLResponse)
def home():
    with connect() as conn:
        version = conn.execute("SHOW server_version").fetchone()[0]
        rows = conn.execute("SELECT registrant, district, checked_in_by, checked_in_at FROM registrant_checkins "
                            "ORDER BY id DESC LIMIT 10").fetchall()
        total = conn.execute("SELECT count(*) FROM registrant_checkins").fetchone()[0]
    districts = ["Hamilton", "Kingston", "London", "Mississauga", "Ottawa", "Sudbury", "Toronto", "Windsor"]
    trs = "".join(f"<tr><td>{html.escape(r[0])}</td><td>{r[1]}</td><td>{html.escape(r[2] or '')}</td>"
                  f"<td>{r[3]:%Y-%m-%d %H:%M:%S}</td></tr>" for r in rows)
    opts = "".join(f"<option>{d}</option>" for d in districts)
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>OSC Registrant Check-in</title>
<style>body{{font-family:system-ui;max-width:760px;margin:40px auto;color:#111}}
table{{border-collapse:collapse;width:100%}}td,th{{border-bottom:1px solid #ddd;padding:6px;text-align:left;font-size:14px}}
.k{{color:#4f46e5;font-size:12px;letter-spacing:.1em;text-transform:uppercase;font-weight:600}}
input,select,button{{padding:6px;font-size:14px}}button{{background:#4f46e5;color:#fff;border:0;border-radius:4px;padding:7px 14px}}</style>
</head><body>
<p class="k">Ontario Securities Commission &middot; deployed with a bundle</p>
<h1>Registrant Check-in</h1>
<p>App <b>{html.escape(os.getenv("DATABRICKS_APP_NAME", ""))}</b> on Lakebase endpoint
<code>{html.escape(os.environ["LAKEBASE_ENDPOINT"])}</code> (Postgres {version}). {total} check-ins so far.</p>
<form method="post" action="/checkin">
<input name="registrant" placeholder="Registrant / NRD number" required>
<select name="district">{opts}</select> <button>Check in</button></form>
<h3>Latest check-ins</h3><table><tr><th>Registrant</th><th>District</th><th>By</th><th>When</th></tr>{trs}</table>
</body></html>"""
