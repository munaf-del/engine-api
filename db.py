import os
import psycopg2

def db_conn():
    host = os.getenv("DB_HOST", os.getenv("PGHOST", "127.0.0.1"))
    port = int(os.getenv("DB_PORT", os.getenv("PGPORT", "5432")))
    db = os.getenv("DB_NAME", os.getenv("PGDATABASE", "engine_app"))
    user = os.getenv("DB_USER", os.getenv("PGUSER", "postgres"))
    pw = os.getenv("DB_PASSWORD", os.getenv("PGPASSWORD", ""))

    # 🔥 FIX: use private IP instead of socket
    if host.startswith("/cloudsql"):
        # fallback to private IP
        host = "10.68.80.3"   # ← from your screenshot

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pw
    )

def table_exists(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
            );
            """,
            (table_name,),
        )
        return cur.fetchone()[0]