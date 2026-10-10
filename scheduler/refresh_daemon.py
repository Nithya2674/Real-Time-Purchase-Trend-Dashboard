"""
5-Minute Scheduled Refresh Daemon.
Periodically invokes MapReduce job on trailing 60-minute HDFS data every 5 minutes (300 seconds).
Tracks countdown clock, status, and notifies listeners.
"""

import os
import sys
import time
import threading
from typing import Dict, Any, Optional, Callable, List

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from mapreduce.job_runner import job_runner

class MapReduceRefreshScheduler:
    def __init__(self, interval_seconds: int = 300):
        self.interval_seconds = interval_seconds
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.last_run_timestamp: Optional[float] = None
        self.next_run_timestamp: float = time.time() + self.interval_seconds
        self.execution_count = 0
        self.listeners: List[Callable[[Dict[str, Any]], None]] = []
        self._lock = threading.Lock()

    def add_listener(self, callback: Callable[[Dict[str, Any]], None]):
        with self._lock:
            self.listeners.append(callback)

    def trigger_now(self) -> Dict[str, Any]:
        """Manually trigger immediate execution and reset countdown."""
        result = job_runner.execute_top20_job(window_minutes=60, top_k=20)
        with self._lock:
            self.last_run_timestamp = time.time()
            self.next_run_timestamp = self.last_run_timestamp + self.interval_seconds
            self.execution_count += 1
            for cb in self.listeners:
                try:
                    cb(result)
                except Exception:
                    pass
        return result

    def set_interval(self, seconds: int):
        with self._lock:
            self.interval_seconds = max(10, seconds)
            self.next_run_timestamp = time.time() + self.interval_seconds

    def _loop(self):
        while self.is_running:
            time.sleep(1)
            now = time.time()
            if now >= self.next_run_timestamp:
                try:
                    self.trigger_now()
                except Exception as e:
                    print(f"[Scheduler] Execution error: {e}")
                    with self._lock:
                        self.next_run_timestamp = time.time() + self.interval_seconds

    def start(self):
        if not self.is_running:
            self.is_running = True
            # Execute first run immediately to establish baseline
            try:
                self.trigger_now()
            except Exception:
                pass
            self.thread = threading.Thread(target=self._loop, daemon=True, name="MapReduceSchedulerThread")
            self.thread.start()
            print(f"[Scheduler] Started 5-minute periodic refresh daemon (interval: {self.interval_seconds}s).")

    def stop(self):
        self.is_running = False
        print("[Scheduler] Stopped.")

    def get_status(self) -> Dict[str, Any]:
        now = time.time()
        remaining = max(0, int(self.next_run_timestamp - now))
        return {
            "is_running": self.is_running,
            "interval_seconds": self.interval_seconds,
            "interval_minutes": round(self.interval_seconds / 60, 1),
            "last_run_timestamp": self.last_run_timestamp,
            "next_run_timestamp": self.next_run_timestamp,
            "seconds_until_next_refresh": remaining,
            "execution_count": self.execution_count
        }

# Shared scheduler instance (300 seconds = 5 minutes)
scheduler = MapReduceRefreshScheduler(interval_seconds=300)
