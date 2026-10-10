"""
Kafka Producer for Real-Time Instacart Transactions.
Streams purchase events into 'instacart-transactions' topic.
Connects to real Apache Kafka broker if reachable, or uses embedded broker.
"""

import os
import sys
import json
import time
import threading
import argparse
from typing import Optional, Dict, Any

# Ensure project root is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataset.sample_data import InstacartTransactionGenerator
from kafka_pipeline.broker_simulator import broker

class InstacartKafkaProducer:
    def __init__(self, bootstrap_servers: Optional[str] = None, topic: str = "instacart-transactions"):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = topic
        self.generator = InstacartTransactionGenerator()
        self.real_producer = None
        self.is_running = False
        self.is_paused = False
        self.rate_per_sec = 8.0 # default transactions per second
        self.thread: Optional[threading.Thread] = None
        self.messages_sent = 0
        self.active_surge: Optional[Dict[str, Any]] = None
        self._init_real_kafka()

    def _init_real_kafka(self):
        """Attempts to connect to real Apache Kafka broker."""
        try:
            from kafka import KafkaProducer
            self.real_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=2000,
                max_block_ms=2000
            )
            print(f"[Kafka Producer] Connected successfully to Apache Kafka at {self.bootstrap_servers}")
        except Exception as e:
            self.real_producer = None
            # print(f"[Kafka Producer] Apache Kafka at {self.bootstrap_servers} not reachable. Running on high-speed embedded broker.")

    def send_transaction(self, tx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Generates and delivers a single purchase event to the Kafka topic."""
        if tx is None:
            tx = self.generator.generate_single_transaction()
            
        delivered_to_cluster = False
        if self.real_producer:
            try:
                self.real_producer.send(self.topic, value=tx)
                delivered_to_cluster = True
            except Exception:
                pass

        # Also publish to embedded broker for real-time dashboard pub/sub
        broker.publish(self.topic, tx)
        self.messages_sent += 1
        return tx

    def set_rate(self, tx_per_sec: float):
        """Update transaction rate dynamically."""
        self.rate_per_sec = max(0.5, float(tx_per_sec))

    def trigger_promotion_surge(self, product_id: int, multiplier: float = 4.0, duration_seconds: int = 30):
        """Simulate a flash promotion on a specific product."""
        self.generator.set_promotion(product_id, multiplier)
        self.active_surge = {
            "product_id": product_id,
            "multiplier": multiplier,
            "expires_at": time.time() + duration_seconds
        }
        print(f"[Kafka Producer] Triggered flash promotion on Product ID {product_id} (x{multiplier}) for {duration_seconds}s")

    def _run_loop(self):
        while self.is_running:
            if self.is_paused:
                time.sleep(0.5)
                continue

            # Check if surge has expired
            if self.active_surge and time.time() > self.active_surge["expires_at"]:
                self.generator.set_promotion(None, 1.0)
                self.active_surge = None

            self.send_transaction()

            sleep_interval = 1.0 / self.rate_per_sec
            time.sleep(sleep_interval)

    def start(self):
        if not self.is_running:
            self.is_running = True
            self.is_paused = False
            self.thread = threading.Thread(target=self._run_loop, daemon=True, name="InstacartKafkaProducerThread")
            self.thread.start()
            print(f"[Kafka Producer] Started streaming to topic '{self.topic}' at {self.rate_per_sec} tx/s.")

    def pause(self):
        self.is_paused = True

    def resume(self):
        self.is_paused = False

    def stop(self):
        self.is_running = False
        if self.real_producer:
            try:
                self.real_producer.flush()
                self.real_producer.close()
            except Exception:
                pass
        print("[Kafka Producer] Stopped.")

    def get_status(self) -> Dict[str, Any]:
        return {
            "is_running": self.is_running,
            "is_paused": self.is_paused,
            "topic": self.topic,
            "rate_per_sec": self.rate_per_sec,
            "messages_sent": self.messages_sent,
            "cluster_connected": self.real_producer is not None,
            "active_surge": self.active_surge
        }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Instacart Kafka Transaction Streaming Producer")
    parser.add_argument("--rate", type=float, default=5.0, help="Transactions per second")
    parser.add_argument("--topic", type=str, default="instacart-transactions", help="Kafka topic name")
    args = parser.parse_args()

    producer = InstacartKafkaProducer(topic=args.topic)
    producer.set_rate(args.rate)
    producer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        producer.stop()
