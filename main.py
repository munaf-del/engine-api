from routes.project_routes import router as project_router
from routes.calculator_routes import router as calculator_router, CALCULATOR_TEMPLATES
from routes.db_routes import router as db_router
from routes.health_routes import router as health_router
from run_storage_service import save_run_result
from db import db_conn, table_exists
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from runner_registry import resolve_runner
from psycopg2.extras import RealDictCursor
from runner_service import run_engine, ALLOWED_RUNNERS

app = FastAPI()

app.include_router(health_router)
app.include_router(db_router)
app.include_router(calculator_router)
app.include_router(project_router)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------
# BASIC ENDPOINTS
# -----------------------------

@app.post("/run-and-save/{runner_name}")
def run_and_save(runner_name: str, payload: dict = Body(...)):
    result = run_engine(runner_name, payload, ALLOWED_RUNNERS)

    if isinstance(result, dict) and result.get("error"):
        return result  # don't save failures

    conn = db_conn()
    try:
        save_run_result(conn, result, payload)
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
