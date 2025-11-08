import logging
import sys
import time
from typing import Any
import signal
from base import Worker
from config import config
from mrds import MyRedis
import os
import pandas as pd


class WcWorker(Worker):
    should_exit = False  # shared across all workers
    def run(self, **kwargs: Any) -> None:
        rds: MyRedis = kwargs["rds"]
        # Write the code for the worker thread here.      # Read
        consumer_name = f"{Worker.GROUP}-{os.getpid()}"
        # logging.info(f"{consumer_name} started")
        last_autoclaim = time.time()
        while WcWorker.should_exit != True:
            try:
                msgs = rds.rds.xreadgroup(
                    Worker.GROUP,
                    consumer_name,
                    streams={config["IN"]: ">"},
                    count=1,
                    block=1000,
                )
                # logging.debug(f"{consumer_name} read messages: {msgs}")
                if not msgs:
                    continue
                for stream_name, messages in msgs:
                    for message_id, fields in messages:
                        decoded_fields = {
                            k.decode(): v.decode() for k, v in fields.items()
                        }
                        fname = decoded_fields.get(config["FNAME"])
                        # logging.info(f"{self.name} processing file {fname}")
                        wc = {}
                        if self.crash:
                            logging.info(f"{consumer_name} crashing as requested with file {fname}")
                            os.kill(os.getpid(), signal.SIGKILL)
                        if self.slow:
                            logging.info(f"{consumer_name} sleeping as requested to simulate straggler with file {fname}")
                            time.sleep(10)
                        df = pd.read_csv(fname, lineterminator="\n")
                        df["text"] = df["text"].astype(str)
                        for text in df.loc[:, "text"]:
                            if text == "\n":
                                continue

                            for word in text.split(" "):
                                if word not in wc:
                                    wc[word] = 0
                                wc[word] = wc[word] + 1
                        rds.update_word_counts_and_ack(wc, message_id)
                        logging.info(
                            f"{consumer_name} finished processing file {fname}"
                        )
                      
                
                if time.time() - last_autoclaim <3:
                    continue
                last_autoclaim = time.time()
                result = rds.rds.xautoclaim(
                    config["IN"],
                    Worker.GROUP,
                    consumer_name,
                    min_idle_time=3000,
                    count=1,
                )
                logging.info(f"{consumer_name} autoclaim result: {result}") 
                # Handle either 2- or 3-element return
                if isinstance(result, (list, tuple)) and len(result) >= 2:
                    next_id = result[0]
                    pending_messages = result[1]
                else:
                    next_id, pending_messages = None, []

                if not pending_messages:
                    continue
                for message_id, fields in pending_messages:
                    decoded_fields = {
                        k.decode(): v.decode() for k, v in fields.items()
                    }
                    fname = decoded_fields.get(config["FNAME"])
                    # logging.info(f"{self.name} re-processing file {fname}")
                    wc = {}

                    df = pd.read_csv(fname, lineterminator="\n")
                    df["text"] = df["text"].astype(str)
                    for text in df.loc[:, "text"]:
                        if text == "\n":
                            continue

                        for word in text.split(" "):
                            if word not in wc:
                                wc[word] = 0
                            wc[word] = wc[word] + 1
                    rds.update_word_counts_and_ack(wc, message_id)
                    logging.info(
                        f"{consumer_name} finished re-processing file {fname}"
                    )


            except Exception as e:
                logging.error(f"Error in {consumer_name}: {e}")
            if WcWorker.should_exit:
                sys.exit()
        logging.info(f"Worker {os.getpid()} shutting down cleanly")
        sys.exit(0)
        logging.info(f"Worker {os.getpid()} exiting cleanly.")
        os._exit(0)
    @staticmethod
    def _signal_handler(signum, frame):
        logging.info(f"Worker {os.getpid()} received SIGTERM — shutting down cleanly.")
        WcWorker.should_exit = True
    def kill(self) -> None:
        logging.info(f"Killing worker {self.pid}")
        try:
            os.kill(self.pid,signal.SIGKILL)
        except Exception as e:
            logging.error(f"Failed to kill worker {self.pid}: {e}")
