from typing import Optional

from fastapi import HTTPException
from psycopg2.extras import Json


def save_run_result(conn, result: dict, payload: dict, calc_instance_id: Optional[str] = None):
    run_id = result.get("run_id")
    created_utc = result.get("created_utc")
    project = result.get("project") or {}
    project_id = project.get("id")
    project_name = project.get("name")

    if not run_id or not created_utc:
        raise HTTPException(
            status_code=400,
            detail={"error": "runner output missing run_id/created_utc", "result": result},
        )

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