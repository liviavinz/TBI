"""
Background scheduler that periodically runs TBISync.
"""
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

from sync_tbi import TBISync

log = logging.getLogger(__name__)


def _run_sync():
    """Run a single sync, catch errors so the scheduler keeps running."""
    log.info("Sync triggered")
    try:
        TBISync().run()
        log.info("Sync completed successfully")
    except Exception as e:
        log.error(f"Sync failed: {e}", exc_info=True)


def start_scheduler(interval_minutes: int = 5, run_immediately: bool = True):
    """
    Start a background scheduler that runs the sync every N minutes.

    :param interval_minutes: how often to run the sync
    :param run_immediately: if True, also run once at startup
    :return: the scheduler instance
    """
    scheduler = BackgroundScheduler(daemon=True)

    next_run = datetime.now() if run_immediately else None
    scheduler.add_job(
        _run_sync,
        trigger='interval',
        minutes=interval_minutes,
        next_run_time=next_run,
        id='tbi_sync_job',
        max_instances=1,  # never let two syncs overlap
        coalesce=True,  # if multiple are queued, run only once
    )

    scheduler.start()
    log.info(f"Sync scheduler started (every {interval_minutes} min)")
    return scheduler