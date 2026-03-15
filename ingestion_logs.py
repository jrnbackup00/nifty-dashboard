from database import SessionLocal
from sqlalchemy import text
from database import SessionLocal
from models import IngestionLog
from sqlalchemy import desc


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

def get_last_successful_ingestion():

    db = SessionLocal()

    try:
        log = (
            db.query(IngestionLog)
            .filter(IngestionLog.status == "SUCCESS")
            .order_by(desc(IngestionLog.run_time))
            .first()
        )

        if log:
            return log.run_time

        return None

    finally:
        db.close()