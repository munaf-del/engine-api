from fastapi import APIRouter, Body, HTTPException
from psycopg2.extras import RealDictCursor

from db import db_conn, table_exists

router = APIRouter()


@router.get("/api/projects")
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

@router.post("/api/projects")
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