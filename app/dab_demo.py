"""Lakebase as Code: deploy a Lakebase project (optionally with an app) from a Declarative Automation Bundle.

The app downloads the Databricks CLI on first use and runs `databricks bundle deploy` / `destroy`
as the app service principal. Every deployment is its own bundle state (root_path keyed by the
project id), so several users can deploy side by side; the requesting user gets CAN_MANAGE.
"""
import io
import json
import os
import platform
import re
import shutil
import socket
import stat
import subprocess
import threading
import time
import uuid
import zipfile
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient

router = APIRouter(prefix="/api/dab")
w = WorkspaceClient()

CLI_VERSION = os.getenv("DAB_CLI_VERSION", "1.21.0")
CLI_DIR = Path("/tmp/dab-cli")
CLI_BIN = CLI_DIR / "databricks"
WORK_DIR = Path("/tmp/dab-deployments")
TEMPLATE_DIR = Path(__file__).parent / "dab_templates"
BUNDLE_NAME = "osc-lakebase-dab"
# Free Edition caps projects, storage and concurrently running endpoints: keep the demo small
MAX_DEPLOYMENTS = int(os.getenv("DAB_MAX_DEPLOYMENTS", "2"))

PROJECT_RE = re.compile(r"^[a-z][a-z0-9-]{2,40}[a-z0-9]$")
APP_RE = re.compile(r"^[a-z][a-z0-9-]{0,28}[a-z0-9]$")

_cli_lock = threading.Lock()
_cli_state = {"status": "not_installed", "error": None}
_jobs: dict[str, dict] = {}
_busy: set[str] = set()
_busy_lock = threading.Lock()


LAKEBASE_ONLY_YML = """\
bundle:
  name: {bundle_name}

variables:
  project_id:
    description: Lakebase project id (unique per deployment)
  owner:
    description: User who gets CAN_MANAGE on what this bundle deploys

targets:
  demo:
    default: true
    workspace:
      # One state folder per project, so several people can deploy this bundle side by side
      root_path: /Workspace/Users/${{workspace.current_user.userName}}/.bundle/{bundle_name}/${{var.project_id}}

resources:
  postgres_projects:
    lakebase:
      project_id: ${{var.project_id}}
      display_name: "OSC Lakebase (DAB) - ${{var.project_id}}"
      pg_version: 17
      purge_on_delete: true
      permissions:
        - user_name: ${{var.owner}}
          level: CAN_MANAGE
"""

LAKEBASE_APP_YML = LAKEBASE_ONLY_YML + """\

  apps:
    hello_app:
      name: ${{var.app_name}}
      description: "OSC registrant lookup on Lakebase, deployed from a bundle"
      source_code_path: ./hello_app
      resources:
        # Binding the app to the branch + database creates its Postgres role and grants it access
        - name: db
          postgres:
            branch: ${{resources.postgres_projects.lakebase.name}}/branches/production
            database: ${{resources.postgres_projects.lakebase.name}}/branches/production/databases/databricks-postgres
            permission: CAN_CONNECT_AND_CREATE
      permissions:
        - user_name: ${{var.owner}}
          level: CAN_MANAGE
"""


def _bundle_yml(kind: str) -> str:
    yml = LAKEBASE_APP_YML if kind == "app" else LAKEBASE_ONLY_YML
    if kind == "app":
        yml = yml.replace(
            "  owner:\n",
            "  app_name:\n    description: Databricks App name (unique per workspace)\n  owner:\n",
        )
    return yml.format(bundle_name=BUNDLE_NAME)


# --- Databricks CLI ---


def _cli_arch() -> str:
    m = platform.machine().lower()
    return "arm64" if m in ("aarch64", "arm64") else "amd64"


def _ensure_cli() -> Path:
    with _cli_lock:
        if CLI_BIN.exists():
            return CLI_BIN
        _cli_state.update(status="installing", error=None)
        url = (f"https://github.com/databricks/cli/releases/download/v{CLI_VERSION}/"
               f"databricks_cli_{CLI_VERSION}_linux_{_cli_arch()}.zip")
        try:
            resp = httpx.get(url, follow_redirects=True, timeout=120)
            resp.raise_for_status()
            CLI_DIR.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                zf.extract("databricks", CLI_DIR)
            CLI_BIN.chmod(CLI_BIN.stat().st_mode | stat.S_IEXEC)
            _cli_state.update(status="installed")
        except Exception as e:
            _cli_state.update(status="error", error=f"Could not download the Databricks CLI from {url}: {e}")
            raise
        return CLI_BIN


def _cli_env() -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith("DATABRICKS_CONFIG")}
    # Direct deployment engine: no Terraform download needed
    env["DATABRICKS_BUNDLE_ENGINE"] = "direct"
    env["DATABRICKS_CLI_DO_NOT_TRACK"] = "1"
    host = env.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        env["DATABRICKS_HOST"] = f"https://{host}"
    return env


# --- Bundle files ---


def _write_bundle(kind: str, project_id: str) -> Path:
    d = WORK_DIR / project_id
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    (d / "databricks.yml").write_text(_bundle_yml(kind))
    if kind == "app":
        shutil.copytree(TEMPLATE_DIR / "hello_app", d / "hello_app")
    return d


def _bundle_root() -> str:
    return f"/Workspace/Users/{w.current_user.me().user_name}/.bundle/{BUNDLE_NAME}"


def _marker_path(project_id: str) -> str:
    return f"{_bundle_root()}/{project_id}/dab-demo.json"


def _list_deployments() -> list[dict]:
    from databricks.sdk.errors import NotFound
    out = []
    try:
        entries = list(w.workspace.list(_bundle_root()))
    except NotFound:
        return out
    for e in entries:
        try:
            with w.workspace.download(f"{e.path}/dab-demo.json") as f:
                meta = json.loads(f.read())
        except Exception:
            continue  # no record: validation failed before anything was created
        out.append(meta)
    return sorted(out, key=lambda m: m.get("created", 0))


def _write_marker(meta: dict):
    from databricks.sdk.service.workspace import ImportFormat
    w.workspace.upload(_marker_path(meta["project_id"]), io.BytesIO(json.dumps(meta).encode()),
                       format=ImportFormat.AUTO, overwrite=True)


# --- Background jobs ---


def _run(job: dict, args: list[str], cwd: Path) -> bool:
    job["log"].append(f"$ databricks {' '.join(args)}")
    proc = subprocess.Popen([str(CLI_BIN), *args], cwd=cwd, env=_cli_env(),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
    for line in proc.stdout:
        job["log"].append(line.rstrip())
    proc.wait()
    if proc.returncode != 0:
        job["log"].append(f"exit code {proc.returncode}")
    return proc.returncode == 0


def _check_app(job: dict, url: str) -> bool:
    """Call the deployed app's /health as this app's service principal (it owns the new app)."""
    job["log"].append(f"Checking {url}/health ...")
    for _ in range(12):
        try:
            r = httpx.get(f"{url}/health", headers=w.config.authenticate(), timeout=60, follow_redirects=False)
            if r.status_code == 200:
                job["log"].append(f"App is up and connected to Lakebase: {r.text}")
                return True
            job["log"].append(f"  HTTP {r.status_code}: {r.text[:300]}")
        except Exception as e:
            job["log"].append(f"  {e}")
        time.sleep(10)
    job["log"].append("The app did not pass its health check; see its logs in the workspace (Compute > Apps).")
    return False


def _job_thread(job: dict):
    meta = job["meta"]
    pid = meta["project_id"]
    try:
        if not CLI_BIN.exists():
            job["log"].append(f"Downloading Databricks CLI v{CLI_VERSION} ({_cli_arch()})...")
        _ensure_cli()
        d = _write_bundle(meta["kind"], pid)
        var_args = [f"--var=project_id={pid}", f"--var=owner={meta['owner']}"]
        if meta["kind"] == "app":
            var_args.append(f"--var=app_name={meta['app_name']}")
        ok = True
        if job["action"] == "deploy":
            ok = _run(job, ["bundle", "validate", *var_args], d)
            if ok:
                # Record the deployment before creating anything, so a partial deploy can still be destroyed
                meta["state"] = "deploying"
                _write_marker(meta)
                ok = _run(job, ["bundle", "deploy", "--auto-approve", *var_args], d)
            if ok and meta["kind"] == "app":
                ok = _run(job, ["bundle", "run", "hello_app", *var_args], d)
                if ok:
                    meta["app_url"] = w.apps.get(meta["app_name"]).url
                    ok = _check_app(job, meta["app_url"])
            if meta.get("state"):
                meta["state"] = "deployed" if ok else "failed"
                _write_marker(meta)
            if not ok and any("projects limit exceeded" in line for line in job["log"]):
                job["log"].append("")
                job["log"].append("This workspace is at its Lakebase project limit (Free Edition allows 3 per workspace). "
                                  "Destroy a deployment below, or delete an unused Lakebase project, then deploy again.")
        else:
            ok = _run(job, ["bundle", "destroy", "--auto-approve", *var_args], d)
        job["status"] = "succeeded" if ok else "failed"
    except Exception as e:
        job["log"].append(f"ERROR: {e}")
        job["status"] = "failed"
    finally:
        job["ended"] = time.time()
        with _busy_lock:
            _busy.discard(pid)


def _start_job(action: str, meta: dict) -> dict:
    pid = meta["project_id"]
    with _busy_lock:
        if pid in _busy:
            raise HTTPException(409, f"A deploy or destroy for {pid} is already running")
        _busy.add(pid)
    job = {"id": uuid.uuid4().hex[:12], "action": action, "meta": meta, "status": "running",
           "log": [], "started": time.time(), "ended": None}
    _jobs[job["id"]] = job
    threading.Thread(target=_job_thread, args=(job,), daemon=True).start()
    return job


# --- API ---


class DabRequest(BaseModel):
    kind: str  # "lakebase" or "app"
    project_id: str
    app_name: str | None = None


def _validate(req: DabRequest):
    if req.kind not in ("lakebase", "app"):
        raise HTTPException(400, "kind must be 'lakebase' or 'app'")
    if not PROJECT_RE.match(req.project_id):
        raise HTTPException(400, "Project id: 4-42 chars, lowercase letters, digits and dashes, starting with a letter")
    if req.kind == "app" and not (req.app_name and APP_RE.match(req.app_name)):
        raise HTTPException(400, "App name: 2-30 chars, lowercase letters, digits and dashes, starting with a letter")


def _user(request: Request) -> str:
    return request.headers.get("x-forwarded-email") or w.current_user.me().user_name


@router.get("/status")
def dab_status(request: Request):
    return {
        "cli": {"version": CLI_VERSION, "arch": _cli_arch(), **_cli_state,
                "installed": CLI_BIN.exists()},
        "user": _user(request),
        "max_deployments": MAX_DEPLOYMENTS,
        "deployments": _list_deployments(),
        "jobs": [{k: v for k, v in j.items() if k != "log"} for j in _jobs.values()],
    }


@router.get("/template")
def dab_template(kind: str = "lakebase"):
    files = {"databricks.yml": _bundle_yml("app" if kind == "app" else "lakebase")}
    if kind == "app":
        for f in sorted((TEMPLATE_DIR / "hello_app").iterdir()):
            if f.is_file():
                files[f"hello_app/{f.name}"] = f.read_text()
    return {"files": files}


@router.post("/install-cli")
def dab_install_cli():
    try:
        _ensure_cli()
    except Exception:
        raise HTTPException(502, _cli_state["error"])
    out = subprocess.run([str(CLI_BIN), "--version"], capture_output=True, text=True, env=_cli_env())
    return {"version": out.stdout.strip()}


@router.post("/deploy")
def dab_deploy(req: DabRequest, request: Request):
    _validate(req)
    existing = _list_deployments()
    if req.project_id not in [d["project_id"] for d in existing]:
        if len(existing) >= MAX_DEPLOYMENTS:
            raise HTTPException(409, f"{len(existing)} bundle deployments already exist (limit {MAX_DEPLOYMENTS} "
                                     "on this workspace). Destroy one first.")
        from databricks.sdk.errors import NotFound
        try:
            w.postgres.get_project(name=f"projects/{req.project_id}")
            raise HTTPException(409, f"Lakebase project {req.project_id} already exists; pick another name")
        except NotFound:
            pass
        if req.kind == "app":
            try:
                w.apps.get(req.app_name)
                raise HTTPException(409, f"App {req.app_name} already exists; pick another name")
            except NotFound:
                pass
    meta = {"kind": req.kind, "project_id": req.project_id, "app_name": req.app_name,
            "owner": _user(request), "created": time.time()}
    return {"job_id": _start_job("deploy", meta)["id"]}


@router.post("/destroy/{project_id}")
def dab_destroy(project_id: str):
    meta = next((d for d in _list_deployments() if d["project_id"] == project_id), None)
    if not meta or meta.get("kind") not in ("lakebase", "app"):
        raise HTTPException(404, f"No bundle deployment found for {project_id}")
    return {"job_id": _start_job("destroy", meta)["id"]}


@router.get("/jobs/{job_id}")
def dab_job(job_id: str, offset: int = 0):
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Unknown job")
    return {**{k: v for k, v in job.items() if k != "log"}, "log": job["log"][offset:],
            "next_offset": len(job["log"])}


@router.post("/test/{project_id}")
def dab_test_connection(project_id: str):
    """Connect to the deployed project's production endpoint as the app service principal."""
    branch = f"projects/{project_id}/branches/production"
    try:
        ep = next(iter(w.postgres.list_endpoints(parent=branch)))
    except StopIteration:
        raise HTTPException(404, f"No endpoint on {branch}")
    host = ep.status.hosts.host
    cred = w.postgres.generate_database_credential(endpoint=ep.name)
    t0 = time.time()
    try:
        row = _probe(host, ep.name, cred.token)
    except Exception as e:
        raise HTTPException(502, f"Could not connect to {host}: {e}")
    return {"endpoint": ep.name, "host": host, "user": row[0], "version": row[1].split(" on ")[0],
            "server_time": str(row[2]), "elapsed_ms": round((time.time() - t0) * 1000)}


def _probe(host: str, endpoint: str, token: str):
    import psycopg
    with psycopg.connect(host=host, hostaddr=socket.gethostbyname(host), dbname="databricks_postgres",
                         user=w.current_user.me().user_name, password=token, sslmode="require",
                         connect_timeout=60) as conn:
        return conn.execute("SELECT current_user, version(), now()").fetchone()
