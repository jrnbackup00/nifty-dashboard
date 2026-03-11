from database import SessionLocal
from sqlalchemy import text


def acquire_scheduler_lock():

    db = SessionLocal()

    result = db.execute(
        text("""
        SELECT pg_try_advisory_lock(987654321);
        """)
    ).scalar()

    db.close()

    return result