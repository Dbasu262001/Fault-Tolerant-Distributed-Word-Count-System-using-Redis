import glob
import logging
import os
import signal
import sys
import time
from threading import current_thread

from config import config
from worker import WcWorker
from mrds import MyRedis
from checkpoint import create_checkpoints

workers: list[WcWorker] = []


def sigterm_handler(signum, frame):
    logging.info("Killing main process!")
    for w in workers:
        w.kill()
    sys.exit()


if __name__ == "__main__":
    # Clear the log file
    open(config["LOGFILE"], "w").close()
    logging.basicConfig(  # filename=LOGFILE,
        level=logging.DEBUG,
        force=True,
        format="%(asctime)s [%(threadName)s] %(levelname)s: %(message)s",
    )
    thread = current_thread()
    thread.name = "client"
    logging.debug("Done setting up loggers.")

    rds = MyRedis()

    for file in glob.glob(config["DATA_PATH"]):
        rds.add_file(file)
    signal.signal(signal.SIGTERM, sigterm_handler)


    # Crash half the workers after they read a row
    for i in range(config["N_NORMAL_WORKERS"]):
        workers.append(WcWorker())

    for i in range(config["N_CRASHING_WORKERS"]):
        workers.append(WcWorker(crash=False))

    for i in range(config["N_SLEEPING_WORKERS"]):
        workers.append(WcWorker(slow=False))

    for w in workers:
        w.create_and_run(rds=rds)

    create_checkpoints(rds, config["CHECKPOINT_INTERVAL"])
    logging.debug("Created all the workers")
    time.sleep(1)  # Give workers time to start and read some messages

    while rds.is_pending():
        logging.info("Work still pending, sleeping before checking again...")
        time.sleep(4)
        # rds.restart(downtime=config["REDIS_DOWNTIME"])
    
    logging.info("No more pending work detected.")
    for w in workers:
        w.kill()

    for w in workers:
        try:
            pid, status = os.waitpid(w.pid, 0)  # Block until the worker exits
            logging.debug(f"Worker {pid} exited with status {status}")
        except ChildProcessError:
            logging.warning(f"Worker {w.pid} already exited.")
    for word, c in rds.top(3):
        logging.info(f"{word.decode()}: {c}")
    logging.info("All work complete, exiting.")
    os._exit(0)
