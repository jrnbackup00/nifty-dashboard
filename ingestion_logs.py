from database import SessionLocal
from sqlalchemy import text


def log_ingestion(job_type, status, rows, error=None):

    db = SessionLocal()

    db.execute(
        text("""
        INSERT INTO ingestion_logs
        (run_time, job_type, status, rows_ingested, errors)
        VALUES (NOW(), :job, :status, :rows, :err)
        """),
        {
            "job": job_type,
            "status": status,
            "rows": rows,
            "err": error
        }
    )

    db.commit()
    db.close()