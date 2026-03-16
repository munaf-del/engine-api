from fastapi import APIRouter, Body, HTTPException
from psycopg2.extras import RealDictCursor

from db import db_conn
from runner_registry import resolve_runner
from runner_service import run_engine
from run_storage_service import save_run_result

router = APIRouter()

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


@router.get("/api/calculator-templates")
def list_calculator_templates():
    return CALCULATOR_TEMPLATES

@router.get("/api/projects/{project_id}/calculators")
def list_calculators(project_id: str):
    conn = db_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM calculator_instances WHERE project_id = %s ORDER BY created_utc DESC;",
                (project_id,),
            )
            return {"calculators": cur.fetchall()}
    finally:
        conn.close()

@router.post("/api/projects/{project_id}/calculators")
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

@router.post("/api/calculators/{calc_instance_id}/run-and-save")
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

    # Existing template method
    elif template_key:
        template = next((t for t in CALCULATOR_TEMPLATES if t["key"] == template_key), None)
        if not template:
            raise HTTPException(status_code=404, detail=f"Template with key '{template_key}' not found")
        runner_name = template["runner_name"]

    # Raw runner fallback
    elif runner_key:
        runner_name = runner_key

    if not runner_name:
        raise HTTPException(status_code=404, detail="Could not resolve a valid runner")

    result = run_engine(runner_name, run_payload, {
        "multi_lp_runner_v6_3.py",
        "multi_lp_runner_v7.py",
        "multi_lp_runner_v6.py",
        "multi_lp_runner_v6_3_v2_1.py",
        "multi_lp_runner_v6_3_v2_1_layout.py",
    })

    if isinstance(result, dict) and result.get("error"):
        return result

    conn = db_conn()
    try:
        save_run_result(conn, result, run_payload, calc_instance_id)
        return {"saved": True, "run_id": result.get("run_id"), "result": result}
    finally:
        conn.close()