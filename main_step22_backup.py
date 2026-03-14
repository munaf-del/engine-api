import os
import json
import subprocess
import tempfile
from runner_service import run_engine
from db import db_conn, table_exists
from runner_registry import RUNNER_REGISTRY, resolve_runner
from typing import Any, Dict, Optional, List

from psycopg2.extras import Json, RealDictCursor
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------
# DB CONNECTION
# -----------------------------


# -----------------------------
# RUNNER CONFIG
# -----------------------------

RUNNERS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "engine_core", "runners")
    )

ALLOWED_RUNNERS = {
    "multi_lp_runner_v6_3.py",
    "multi_lp_runner_v7.py",
    "multi_lp_runner_v6.py",
    "multi_lp_runner_v6_3_v2_1.py",
    "multi_lp_runner_v6_3_v2_1_layout.py",
}

CALCULATOR_TEMPLATES = [
    {
        "key": "multi_lp_v6_3",
        "name": "Multi LP v6.3",
        "description": "A description for the Multi LP v6.3 runner.",
        "runner_name": "multi_lp_runner_v6_3.py",
        "calculator_key": "MULTI_PILE_FOUNDATION",
        "engine_version": "v6_3",
    },
    {
        "key": "multi_lp_v7",
        "name": "Multi LP v7",
        "description": "A description for the Multi LP v7 runner.",
        "runner_name": "multi_lp_runner_v7.py",
        "calculator_key": "MULTI_PILE_FOUNDATION",
        "engine_version": "v7",
    },
]


def _runner_path(runner_name: str) -> str:
    return os.path.join(RUNNERS_DIR, runner_name)


# -----------------------------
# BASIC ENDPOINTS
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/db-check")
def db_check():
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            val = cur.fetchone()[0]
        return {"db": "ok", "select_1": val}
    finally:
        conn.close()

# -----------------------------
# RUN + SAVE (INPUT + RESULT)
# -----------------------------
def _save_run_result(conn, result: dict, payload: dict, calc_instance_id: Optional[str] = None):
    run_id = result.get("run_id")
    created_utc = result.get("created_utc")
    project = result.get("project") or {}
    project_id = project.get("id")
    project_name = project.get("name")

    if not run_id or not created_utc:
        raise HTTPException(status_code=400, detail={"error": "runner output missing run_id/created_utc", "result": result})

    with conn.cursor() as cur:
        # store full input + output
        cur.execute(
            """
            INSERT INTO engine_runs (run_id, project_id, project_name, created_utc, input_payload, result_payload, calc_instance_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                project_id=EXCLUDED.project_id,
                project_name=EXCLUDED.project_name,
                created_utc=EXCLUDED.created_utc,
                input_payload=EXCLUDED.input_payload,
                result_payload=EXCLUDED.result_payload,
                calc_instance_id=EXCLUDED.calc_instance_id
            """,
            (run_id, project_id, project_name, created_utc, Json(payload), Json(result), calc_instance_id),
        )

        # optional: clear existing rows for re-save of same run_id
        cur.execute("DELETE FROM engine_pile_results WHERE run_id=%s;", (run_id,))
        cur.execute("DELETE FROM engine_pile_envelopes WHERE run_id=%s;", (run_id,))

        # flatten per-load-point pile results
        by_lp = result.get("results_by_load_point") or {}
        for lp_id, lp_res in by_lp.items():
            if not isinstance(lp_res, dict):
                continue
            for pile_id, axial in lp_res.items():
                cur.execute(
                    """
                    INSERT INTO engine_pile_results (run_id, load_point_id, pile_id, axial)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (run_id, str(lp_id), str(pile_id), float(axial)),
                )

        # flatten envelopes
        env = result.get("envelopes") or {}
        comp = env.get("compression_max") or {}
        tens = env.get("tension_min") or {}
        pile_ids = set(comp.keys()) | set(tens.keys())

        for pile_id in pile_ids:
            cur.execute(
                """
                INSERT INTO engine_pile_envelopes (run_id, pile_id, comp_max, tens_min)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    run_id,
                    str(pile_id),
                    float(comp[pile_id]) if pile_id in comp else None,
                    float(tens[pile_id]) if pile_id in tens else None,
                ),
            )
    conn.commit()


@app.post("/run-and-save/{runner_name}")
def run_and_save(runner_name: str, payload: dict = Body(...)):
    result = run_engine(runner_name, payload, ALLOWED_RUNNERS)

    if isinstance(result, dict) and result.get("error"):
        return result  # don't save failures

    conn = db_conn()
    try:
        _save_run_result(conn, result, payload)
        return {"saved": True, "run_id": result.get("run_id"), "result": result}
    finally:
        conn.close()


# -----------------------------
# READ SAVED RUNS
# -----------------------------
@app.get("/runs")
def list_runs(limit: int = 20):
    limit = max(1, min(int(limit), 200))

    conn = db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT run_id, project_id, project_name, created_utc
                FROM engine_runs
                ORDER BY created_utc DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return {"count": len(rows), "runs": rows}
    finally:
        conn.close()


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    conn = db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT run_id, project_id, project_name, created_utc, input_payload, result_payload, calc_instance_id
                FROM engine_runs
                WHERE run_id=%s
                """,
                (run_id,),
            )
            row = cur.fetchone()
            if not row:
                return {"error": "not found", "run_id": run_id}

        return {"run": row}
    finally:
        conn.close()

# -----------------------------
# CALCS API
# -----------------------------

@app.get("/api/calculator-templates")
def list_calculator_templates():
    return CALCULATOR_TEMPLATES

@app.get("/api/projects")
def list_projects():
    conn = db_conn()
    try:
        if table_exists(conn, "projects"):
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM projects ORDER BY created_at DESC;")
                return {"projects": cur.fetchall()}
        else:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT DISTINCT project_id, project_name FROM engine_runs ORDER BY project_name;")
                return {"projects": cur.fetchall()}
    finally:
        conn.close()

@app.post("/api/projects")
def create_project(payload: dict = Body(...)):
    conn = db_conn()
    try:
        if not table_exists(conn, "projects"):
            raise HTTPException(status_code=400, detail="projects table does not exist")
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO projects (name, client, address) VALUES (%s, %s, %s) RETURNING *;",
                (payload.get("name"), payload.get("client"), payload.get("address"))
            )
            new_project = cur.fetchone()
            conn.commit()
            return new_project
    finally:
        conn.close()

@app.get("/api/projects/{project_id}/calculators")
def list_calculators(project_id: str):
    conn = db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM calculator_instances WHERE project_id = %s ORDER BY created_utc DESC;", (project_id,))
            return {"calculators": cur.fetchall()}
    finally:
        conn.close()

@app.post("/api/projects/{project_id}/calculators")
def create_calculator(project_id: str, payload: dict = Body(...)):
    template_key = payload.get("template_key")
    if not template_key:
        raise HTTPException(status_code=400, detail="template_key is required")

    conn = db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO calculator_instances (project_id, template_key, display_name) VALUES (%s, %s, %s) RETURNING *;",
                (project_id, template_key, payload.get("display_name"))
            )
            new_calc = cur.fetchone()
            conn.commit()
            return new_calc
    finally:
        conn.close()

@app.post("/api/calculators/{calc_instance_id}/run-and-save")
def run_and_save_calculator(calc_instance_id: str, payload: dict = Body(...)):
    run_payload = payload.get("payload", {})
    template_key = payload.get("template_key")
    runner_key = payload.get("runner_key")
    calculator_key = payload.get("calculator_key")
    engine_version = payload.get("engine_version")

    if not ((calculator_key and engine_version) or template_key or runner_key):
        raise HTTPException(
            status_code=400,
            detail="Provide either calculator_key + engine_version, or template_key, or runner_key"
        )
   
    runner_name = None

    # New registry method
    if calculator_key and engine_version:
        try:
            runner_name = resolve_runner(calculator_key, engine_version)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))

    # Existing template method (kept for compatibility)
    elif template_key:
        template = next((t for t in CALCULATOR_TEMPLATES if t["key"] == template_key), None)
        if not template:
            raise HTTPException(status_code=404, detail=f"Template with key '{template_key}' not found")
        runner_name = template["runner_name"]

    # Raw runner fallback
    elif runner_key:
        if runner_key in ALLOWED_RUNNERS:
            runner_name = runner_key
    
    if not runner_name:
        raise HTTPException(status_code=404, detail="Could not resolve a valid runner")

    result = run_engine(runner_name, run_payload, ALLOWED_RUNNERS)

    if isinstance(result, dict) and result.get("error"):
        return result

    conn = db_conn()
    try:
        _save_run_result(conn, result, run_payload, calc_instance_id)
        return {"saved": True, "run_id": result.get("run_id"), "result": result}
    finally:
        conn.close()
