#!/usr/bin/env python3
"""
Periodic checker for true_fail queue items. Logs a warning if any exist.
Intended to be run by a systemd timer or cron.
"""
import os
import sys
import logging
import logging.handlers
from pending_queue import PendingCommentQueue
import datetime

TEST_DB = '/tmp/test_pending_queue.db'
if os.path.exists(TEST_DB):
    DB_PATH = TEST_DB
else:
    from config.config import Config
    DB_PATH = Config.PENDING_QUEUE_DB_PATH

LOGFILE = '/tmp/true_fail_checker.log'
logger = logging.getLogger("true_fail_checker")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
file_handler = logging.FileHandler(LOGFILE)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
try:
    syslog_handler = logging.handlers.SysLogHandler(address = '/var/run/syslog' if sys.platform == 'darwin' else '/dev/log')
    syslog_handler.setFormatter(formatter)
    logger.addHandler(syslog_handler)
except Exception:
    pass

def main():
    try:
        queue = PendingCommentQueue(DB_PATH)
        true_fails = queue.get_true_fail_items(limit=100)
    except Exception as e:
        logger.error(f"DB error: {e}")
        sys.exit(1)
    if true_fails:
        logger.warning(f"ALERT: {len(true_fails)} true_fail items in queue as of {datetime.datetime.now().isoformat()}")
        for item in true_fails:
            logger.warning(f"  ID: {item['id']}, Event: {item['event_id']}, Last error: {item['last_error']}")
    else:
        logger.info("No true_fail items in queue.")

if __name__ == '__main__':
    main()

