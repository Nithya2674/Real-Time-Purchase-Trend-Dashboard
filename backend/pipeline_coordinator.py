"""
Pipeline Coordinator.
Orchestrates the Kafka Producer, Kafka Consumer, HDFS Storage, MapReduce Runner, and Scheduler.
Pre-seeds initial historical data for trailing 60 minutes so the dashboard is instantly live.
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dataset.sample_data import InstacartTransactionGenerator
from kafka_pipeline.broker_simulator import broker
from kafka_pipeline.producer import InstacartKafkaProducer
from kafka_pipeline.consumer import InstacartHDFSConsumer
from hdfs_layer.hdfs_manager import hdfs_manager
from mapreduce.job_runner import job_runner
from scheduler.refresh_daemon import scheduler

class PipelineCoordinator:
    def __init__(self):
        self.producer = InstacartKafkaProducer()
        self.consumer = InstacartHDFSConsumer()
        self.generator = InstacartTransactionGenerator()
        self.is_initialized = False

    def initialize_and_seed(self):
        """Seeds initial trailing 60-minute transaction history into HDFS if not already present."""
        if self.is_initialized:
            return

        existing_trailing = hdfs_manager.get_trailing_records(minutes=60)
        if len(existing_trailing) < 300:
            print(f"[Coordinator] Seeding {1200} historical transactions across trailing 60 minutes into HDFS...")
            seed_events = self.generator.generate_trailing_window(minutes=60, total_events=1200)
            
            # Batch into chunks of 100 to simulate HDFS block partitions
            chunk_size = 100
            for i in range(0, len(seed_events), chunk_size):
                chunk = seed_events[i:i + chunk_size]
                hdfs_manager.write_transaction_batch(chunk)
            print(f"[Coordinator] Pre-seeded HDFS with {len(seed_events)} records across trailing 60 minutes.")

        # Execute baseline MapReduce job
        job_runner.execute_top20_job(window_minutes=60, top_k=20)

        # Start background components
        self.consumer.start()
        self.producer.start()
        self.scheduler.start() if hasattr(self, 'scheduler') else scheduler.start()

        self.is_initialized = True
        print("[Coordinator] Complete pipeline initialized: Kafka -> HDFS -> MapReduce -> 5-min Scheduler.")

    def get_top20_dashboard_data(self) -> Dict[str, Any]:
        """Provides consolidated dashboard payload for Category Managers."""
        top_data = job_runner.get_latest_top20()
        if not top_data:
            top_data = job_runner.execute_top20_job(window_minutes=60, top_k=20)

        # Calculate department summary
        dept_summary = {}
        for p in top_data.get("top_products", []):
            dept = p["department"]
            dept_summary[dept] = dept_summary.get(dept, 0) + p["units_sold"]

        broker_stats = broker.get_stats()
        hdfs_stats = hdfs_manager.get_hdfs_metadata()
        scheduler_stats = scheduler.get_status()
        producer_stats = self.producer.get_status()

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scheduler": scheduler_stats,
            "pipeline_status": {
                "kafka_broker": "ACTIVE",
                "kafka_producer": "STREAMING" if producer_stats["is_running"] and not producer_stats["is_paused"] else "PAUSED",
                "kafka_consumer": "STREAMING_TO_HDFS" if self.consumer.is_running else "STOPPED",
                "hdfs_cluster": hdfs_stats["health"],
                "mapreduce_engine": "READY"
            },
            "metrics": {
                "trailing_window_minutes": 60,
                "refresh_interval_minutes": 5,
                "total_units_sold": top_data["metrics"]["total_units_sold"],
                "total_revenue": top_data["metrics"]["total_revenue"],
                "unique_active_products": top_data["metrics"]["unique_active_products"],
                "current_stream_rate_tps": broker_stats["current_rate_per_sec"],
                "total_transactions_ingested": hdfs_stats["total_records_stored"],
                "hdfs_used_mb": hdfs_stats["used_capacity_mb"]
            },
            "department_breakdown": dept_summary,
            "top_products": top_data.get("top_products", []),
            "last_mapreduce_job": {
                "job_id": top_data["job_id"],
                "executed_at": top_data["executed_at"],
                "duration_ms": top_data["execution_duration_ms"],
                "map_input_records": top_data["metrics"]["map_input_records"],
                "reduce_input_groups": top_data["metrics"]["reduce_input_groups"]
            }
        }

    def get_minute_trends(self, top_n: int = 5) -> Dict[str, Any]:
        """
        Computes minute-by-minute sales trends for the top products over the trailing 60 minutes.
        Shows exact shift in popularity that category managers need!
        """
        records = hdfs_manager.get_trailing_records(minutes=60)
        top_data = job_runner.get_latest_top20()
        top_products = top_data.get("top_products", [])[:top_n] if top_data else []
        target_product_ids = {p["product_id"]: p["product_name"] for p in top_products}

        now_epoch = time.time()
        # Create 12 5-minute buckets or 15 4-minute buckets
        buckets = 12
        bucket_size_seconds = 300 # 5 minutes per bucket
        timeline_labels = []
        for i in range(buckets, 0, -1):
            t = datetime.fromtimestamp(now_epoch - i * bucket_size_seconds, timezone.utc)
            timeline_labels.append(t.strftime("%H:%M"))

        product_series = {p_id: [0] * buckets for p_id in target_product_ids}

        for r in records:
            p_id = r.get("product_id")
            if p_id in target_product_ids:
                epoch = r.get("epoch_seconds", 0)
                diff = now_epoch - epoch
                if 0 <= diff <= (buckets * bucket_size_seconds):
                    bucket_idx = min(buckets - 1, max(0, int((buckets * bucket_size_seconds - diff) // bucket_size_seconds)))
                    product_series[p_id][bucket_idx] += r.get("quantity", 1)

        series = []
        for p_id, counts in product_series.items():
            series.append({
                "product_id": p_id,
                "product_name": target_product_ids[p_id],
                "data": counts
            })

        return {
            "labels": timeline_labels,
            "series": series
        }

# Singleton instance
coordinator = PipelineCoordinator()
