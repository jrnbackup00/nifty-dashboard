from apscheduler.schedulers.background import BackgroundScheduler
from ingest_candles import run_intraday_ingestion
from ingest_candles import run_market_close_ingestion
from scheduler_lock import acquire_scheduler_lock
from zoneinfo import ZoneInfo

def start_scheduler():

    if not acquire_scheduler_lock():
        print("Scheduler already running on another instance")
        return

    print("Scheduler lock acquired — starting jobs")

    scheduler = BackgroundScheduler(timezone=ZoneInfo("Asia/Kolkata"))


    # 11:20
    scheduler.add_job(
        run_intraday_ingestion,
        "cron",
        hour=11,
        minute=20
    )

    # 13:20
    scheduler.add_job(
        run_intraday_ingestion,
        "cron",
        hour=13,
        minute=20
    )

    # 15:20
    scheduler.add_job(
        run_intraday_ingestion,
        "cron",
        hour=15,
        minute=20
    )

    # 16:00
    scheduler.add_job(
        run_market_close_ingestion,
        "cron",
        hour=16,
        minute=0
    )

    scheduler.start()

    print("Scheduler started (Asia/Kolkata timezone)")