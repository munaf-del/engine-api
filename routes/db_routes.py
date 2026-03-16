from fastapi import APIRouter
from db import db_conn

router = APIRouter()


@router.get("/db-check")
def db_check():
    conn = db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            val = cur.fetchone()[0]
        return {"db": "ok", "select_1": val}
    finally:
        conn.close()