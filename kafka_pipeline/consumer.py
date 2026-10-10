"""
Kafka Consumer for Ingesting Instacart Transactions to Hadoop HDFS.
Subscribes to 'instacart-transactions' topic, micro-batches events,
and commits them into partitioned HDFS directory blocks.
"""

import os
import sys
import json
import time
import queue
import threading
import argparse
from typing import List, Dict, Any, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from kafka_pipeline.broker_simulator import broker
from hdfs_layer.hdfs_manager import hdfs_manager

class InstacartHDFSConsumer:
    def __init__(self, topic: str = "instacart-transactions", batch_size: int = 25, flush_interval_secs: float = 2.0):
        self.topic = topic
        self.batch_size = batch_size
        self.flush_interval_secs = flush_interval_secs
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.queue: queue.Queue = broker.subscribe(self.topic)
        self.messages_consumed = 0
        self.batches_written = 0
        self.last_flush_time = time.time()
        self.current_batch: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def _flush_batch(self):
        with self._lock:
            if not self.current_batch:
                return
            batch_to_write = list(self.current_batch)
            self.current_batch = []
            self.last_flush_time = time.time()

        filepath = hdfs_manager.write_transaction_batch(batch_to_write)
        self.batches_written += 1
        # print(f"[HDFS Consumer] Flushed batch of {len(batch_to_write)} records to {filepath}")

    def _run_loop(self):
        while self.is_running:
            try:
                # Poll message with timeout to allow flush_interval_secs check
                try:
                    msg = self.queue.get(timeout=0.5)
                    with self._lock:
                        self.current_batch.append(msg)
                        self.messages_consumed += 1

                    if len(self.current_batch) >= self.batch_size:
                        self._flush_batch()
                except queue.Empty:
                    pass

                # Time-based flush
                if self.current_batch and (time.time() - self.last_flush_time >= self.flush_interval_secs):
                    self._flush_batch()

            except Exception as e:
                print(f"[HDFS Consumer] Error: {e}")
                time.sleep(1)

        # Final flush on exit
        if self.current_batch:
            self._flush_batch()

    def start(self):
        if not self.is_running:
            self.is_running = True
            self.thread = threading.Thread(target=self._run_loop, daemon=True, name="InstacartHDFSConsumerThread")
            self.thread.start()
            print(f"[HDFS Consumer] Subscribed to topic '{self.topic}'. Streaming to HDFS...")

    def stop(self):
        self.is_running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=3)
        print("[HDFS Consumer] Stopped.")

    def get_status(self) -> Dict[str, Any]:
        return {
            "is_running": self.is_running,
            "topic": self.topic,
            "messages_consumed": self.messages_consumed,
            "batches_written": self.batches_written,
            "pending_in_buffer": len(self.current_batch),
            "target_hdfs": hdfs_manager.data_dir
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instacart Kafka to HDFS Consumer")
    parser.add_argument("--batch-size", type=int, default=25, help="Micro-batch size for HDFS write")
    args = parser.parse_args()

    consumer = InstacartHDFSConsumer(batch_size=args.batch_size)
    consumer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        consumer.stop()
