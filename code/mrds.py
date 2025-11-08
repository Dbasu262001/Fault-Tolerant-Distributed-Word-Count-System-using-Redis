from __future__ import annotations

import logging
from typing import Optional, Final

from redis.client import Redis
import time
from base import Worker
from config import config
import os


class MyRedis:
    def __init__(self):
        self.rds: Final = Redis(
            host="localhost", port=6380, password=None, db=0, decode_responses=False
        )
        self.rds.flushall()
        self.rds.xgroup_create(config["IN"], Worker.GROUP, id="0", mkstream=True)
        lua_path = os.path.join(os.path.dirname(__file__), "mylib.lua")
        with open(lua_path, "r") as f:
            lua_script = f.read()
        self.update_and_ack_script = self.rds.register_script(lua_script)

    def add_file(self, fname: str):
        self.rds.xadd(config["IN"], {config["FNAME"]: fname})

    def update_word_counts_and_ack(self, wc: dict[str, int], message_id: str):
        """Atomically update word counts and acknowledge message."""
        args = [Worker.GROUP, message_id]
        for word, count in wc.items():
            args.extend([word, count])

        logging.debug(f"Running Lua with KEYS={ [config['COUNT'], config['IN']] }")
        result = self.update_and_ack_script(
            keys=[config["COUNT"], config["IN"]], args=args
        )
        logging.debug(f"Lua result: {result}")

    def top(self, n: int) -> list[tuple[bytes, float]]:
        return self.rds.zrevrangebyscore(
            config["COUNT"], "+inf", "-inf", 0, n, withscores=True
        )

    def is_pending(self) -> bool:
        try:
            # Get pending summary
            pending_summary = self.rds.xlen(config["IN"])

            if pending_summary == 0:
                return False

            # Fetch detailed pending message list
            details = self.rds.xpending_range(
                config["IN"], Worker.GROUP, "-", "+", pending_summary
            )

            for msg_id, consumer, idle, delivered in details:
                logging.info(
                    f"  ID={msg_id.decode() if isinstance(msg_id, bytes) else msg_id}, "
                    f"Consumer={consumer.decode() if isinstance(consumer, bytes) else consumer}, "
                    f"Idle={idle}ms, Delivered={delivered} times"
                )
            return True
        except Exception as e:
            logging.DEBUG(f"Error fetching pending messages: {e}")
            return False

    def restart(self, downtime: int):
        logging.info(f"Simulating Redis downtime for {downtime}s")
        time.sleep(downtime)
