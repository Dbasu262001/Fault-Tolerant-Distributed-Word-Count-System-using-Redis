from __future__ import annotations

import logging
import os
import signal
import sys
from abc import abstractmethod, ABC
from threading import current_thread
from typing import Any, Final
from multiprocessing import Process
from config import config
import pandas as pd
import time


class Worker(ABC):
    GROUP: Final = "worker"

    def __init__(self, **kwargs: Any):
        self.name = "worker-?"
        self.pid = -1
        self.crash = kwargs["crash"] if "crash" in kwargs else False
        self.slow = kwargs["slow"] if "slow" in kwargs else False
        self.process: Process | None 
    def is_alive(self) -> bool:
        return self.process.is_alive() if self.process else False
    def create_and_run(self, **kwargs: Any) -> None:
        proc = Process(target=self.run, kwargs=kwargs)
        proc.start()
        self.pid = proc.pid
        self.process = proc
        self.name = f"worker-{self.pid}"
        logging.info(f"Started {self.name} with pid {self.pid}")

    @abstractmethod
    def run(self, **kwargs: Any) -> None:
        pass

    def kill(self) -> None:
        logging.info(f"Killing {self.name}")
        try:
            os.kill(self.pid, signal.SIGTERM)  # Try graceful termination first
            # Optionally, wait and escalate to SIGKILL if not terminated
        except Exception as e:
            logging.error(f"Failed to kill {self.name}: {e}")
        logging.info(f"Killed {self.name}")
