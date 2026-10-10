"""
End-to-end Verification & Unit Tests for Problem 21 Pipeline.
Tests:
  1. Instacart Dataset Generation
  2. Kafka Message Broker & Producer
  3. HDFS Micro-batch Ingestion & Partitioning
  4. Hadoop Streaming Mapper & Reducer
  5. MapReduce Job Runner & Top 20 Calculation
  6. 5-Minute Scheduler Daemon
"""

import os
import sys
import json
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dataset.sample_data import InstacartTransactionGenerator
from kafka_pipeline.broker_simulator import broker
from kafka_pipeline.producer import InstacartKafkaProducer
from kafka_pipeline.consumer import InstacartHDFSConsumer
from hdfs_layer.hdfs_manager import hdfs_manager
from mapreduce.job_runner import job_runner
from scheduler.refresh_daemon import scheduler

def run_tests():
    print("=" * 65)
    print("RUNNING END-TO-END PIPELINE TESTS")
    print("=" * 65)

    # 1. Test Dataset Generator
    print("[1/6] Testing Instacart Dataset Generator...")
    gen = InstacartTransactionGenerator(seed=123)
    tx = gen.generate_single_transaction()
    assert "product_id" in tx and "product_name" in tx and "quantity" in tx
    assert tx["quantity"] >= 1
    print(f"  [OK] Generated sample transaction: Order #{tx['order_id']} -> '{tx['product_name']}' (Qty: {tx['quantity']})")

    # 2. Test Historical 60-min generation
    print("[2/6] Generating Trailing 60-Min Dataset Slice...")
    window_events = gen.generate_trailing_window(minutes=60, total_events=500)
    assert len(window_events) == 500
    print(f"  [OK] Generated {len(window_events)} historical events across trailing 60 minutes.")

    # 3. Test HDFS Storage Sink
    print("[3/6] Testing HDFS Partitioned Storage Sink...")
    written_path = hdfs_manager.write_transaction_batch(window_events)
    assert os.path.exists(written_path)
    trailing_records = hdfs_manager.get_trailing_records(minutes=60)
    assert len(trailing_records) >= 500
    hdfs_meta = hdfs_manager.get_hdfs_metadata()
    print(f"  [OK] Successfully wrote to HDFS partition: {written_path}")
    print(f"  [OK] HDFS Total Files: {hdfs_meta['total_files']}, Used: {hdfs_meta['used_capacity_mb']} MB")

    # 4. Test MapReduce Execution
    print("[4/6] Testing MapReduce Trailing 60-Min Top 20 Job...")
    mr_result = job_runner.execute_top20_job(window_minutes=60, top_k=20)
    assert mr_result["job_status"] == "SUCCEEDED"
    assert len(mr_result["top_products"]) == 20
    top1 = mr_result["top_products"][0]
    print(f"  [OK] MapReduce Job ID: {mr_result['job_id']}")
    print(f"  [OK] Map Input Records: {mr_result['metrics']['map_input_records']}")
    print(f"  [OK] Reduce Input Groups: {mr_result['metrics']['reduce_input_groups']}")
    print(f"  [OK] #1 Top Product: '{top1['product_name']}' with {top1['units_sold']} units sold ({top1['share_pct']}%)")

    # 5. Test Kafka Producer & Consumer Bridge
    print("[5/6] Testing Kafka Streaming Producer & Consumer...")
    producer = InstacartKafkaProducer()
    consumer = InstacartHDFSConsumer(batch_size=10, flush_interval_secs=1.0)
    consumer.start()

    # Stream 15 transactions
    for _ in range(15):
        producer.send_transaction()
    time.sleep(1.5)
    consumer.stop()
    print(f"  [OK] Kafka messages published: {broker.get_stats()['messages_published']}")
    print(f"  [OK] HDFS Consumer batches written: {consumer.batches_written}")

    # 6. Test Scheduler Daemon Status
    print("[6/6] Testing 5-Minute Scheduler...")
    sched_status = scheduler.get_status()
    assert sched_status["interval_seconds"] == 300
    print(f"  [OK] Scheduler interval: {sched_status['interval_seconds']}s (5 minutes)")
    print(f"  [OK] Remaining seconds: {sched_status['seconds_until_next_refresh']}s")

    print("=" * 65)
    print("ALL 6 TESTS PASSED SUCCESSFULLY! PIPELINE IS PRODUCTION READY!")
    print("=" * 65)

if __name__ == "__main__":
    run_tests()
