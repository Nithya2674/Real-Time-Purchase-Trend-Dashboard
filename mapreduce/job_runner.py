"""
MapReduce Job Runner & Orchestrator.
Executes the MapReduce pipeline over the trailing 60-minute window.
Emulates full Hadoop cluster job lifecycle with metrics, counters, and HDFS output persistence.
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from hdfs_layer.hdfs_manager import hdfs_manager

class MapReduceJobRunner:
    def __init__(self, log_path: str = "storage/mapreduce_job_history.json"):
        self.log_path = log_path
        self.last_job_result: Optional[Dict[str, Any]] = None
        self.last_run_time: float = 0.0
        self.job_history: List[Dict[str, Any]] = []
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        self._load_history()

    def _load_history(self):
        if os.path.exists(self.log_path):
            try:
                with open(self.log_path, "r", encoding="utf-8") as f:
                    self.job_history = json.load(f)
                    if self.job_history:
                        self.last_job_result = self.job_history[-1]
            except Exception:
                self.job_history = []

    def _save_history(self):
        try:
            with open(self.log_path, "w", encoding="utf-8") as f:
                json.dump(self.job_history[-20:], f, indent=2)
        except Exception:
            pass

    def execute_top20_job(self, window_minutes: int = 60, top_k: int = 20) -> Dict[str, Any]:
        """
        Executes the Hadoop MapReduce job over the trailing 60-minute transaction records on HDFS.
        Records exact MapReduce job counters (Map input, Map output, Shuffle, Reduce input groups).
        """
        job_id = f"job_mr_{int(time.time()*1000)}"
        start_ts = time.time()
        print(f"[MapReduce Runner] Initiating MapReduce job {job_id} for trailing {window_minutes} mins...")

        # 1. HDFS Input Split Phase
        trailing_records = hdfs_manager.get_trailing_records(minutes=window_minutes)
        map_input_records = len(trailing_records)

        # 2. Map Phase
        map_start = time.time()
        intermediate_pairs = []
        cutoff_epoch = start_ts - (window_minutes * 60)

        for record in trailing_records:
            epoch = record.get("epoch_seconds", 0)
            if epoch >= cutoff_epoch:
                product_id = record.get("product_id")
                qty = record.get("quantity", 1)
                name = record.get("product_name", "Unknown")
                dept = record.get("department", "other")
                aisle = record.get("aisle", "other")
                price = record.get("total_price", 0.0)
                intermediate_pairs.append((product_id, qty, name, dept, aisle, price))

        map_output_records = len(intermediate_pairs)
        map_duration_ms = round((time.time() - map_start) * 1000, 2)

        # 3. Shuffle & Sort Phase
        shuffle_start = time.time()
        # Sort by key (product_id)
        intermediate_pairs.sort(key=lambda x: x[0])
        shuffle_duration_ms = round((time.time() - shuffle_start) * 1000, 2)

        # 4. Reduce Phase
        reduce_start = time.time()
        product_aggregates = {}
        total_units = 0
        total_revenue = 0.0

        for p_id, qty, name, dept, aisle, price in intermediate_pairs:
            if p_id not in product_aggregates:
                product_aggregates[p_id] = {
                    "product_id": p_id,
                    "product_name": name,
                    "department": dept,
                    "aisle": aisle,
                    "units_sold": 0,
                    "revenue": 0.0
                }
            product_aggregates[p_id]["units_sold"] += qty
            product_aggregates[p_id]["revenue"] += price
            total_units += qty
            total_revenue += price

        reduce_input_groups = len(product_aggregates)

        # Sort aggregated products descending by units_sold, then revenue
        sorted_products = list(product_aggregates.values())
        sorted_products.sort(key=lambda x: (x["units_sold"], x["revenue"]), reverse=True)

        top_products = sorted_products[:top_k]

        # Calculate rank and percentage share
        for rank, p in enumerate(top_products, start=1):
            p["rank"] = rank
            p["share_pct"] = round((p["units_sold"] / max(1, total_units)) * 100, 2)
            p["revenue"] = round(p["revenue"], 2)
            p["avg_price"] = round(p["revenue"] / max(1, p["units_sold"]), 2)

        reduce_duration_ms = round((time.time() - reduce_start) * 1000, 2)
        total_duration_ms = round((time.time() - start_ts) * 1000, 2)

        job_result = {
            "job_id": job_id,
            "job_status": "SUCCEEDED",
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "execution_duration_ms": total_duration_ms,
            "trailing_window_minutes": window_minutes,
            "metrics": {
                "map_input_records": map_input_records,
                "map_output_records": map_output_records,
                "map_tasks_completed": max(1, map_input_records // 500 + 1),
                "map_time_ms": map_duration_ms,
                "shuffle_time_ms": shuffle_duration_ms,
                "reduce_input_groups": reduce_input_groups,
                "reduce_output_records": len(top_products),
                "reduce_time_ms": reduce_duration_ms,
                "total_units_sold": total_units,
                "total_revenue": round(total_revenue, 2),
                "unique_active_products": reduce_input_groups
            },
            "top_products": top_products
        }

        # 5. Persist output to HDFS
        hdfs_manager.write_mapreduce_output(job_result)

        self.last_job_result = job_result
        self.last_run_time = time.time()
        self.job_history.append(job_result)
        self._save_history()

        print(f"[MapReduce Runner] Job {job_id} completed successfully in {total_duration_ms}ms. Found {len(top_products)} top products.")
        return job_result

    def get_latest_top20(self) -> Optional[Dict[str, Any]]:
        if self.last_job_result:
            return self.last_job_result
        # Fallback to reading from HDFS output file
        return hdfs_manager.get_latest_mapreduce_output()

# Shared job runner instance
job_runner = MapReduceJobRunner()

if __name__ == "__main__":
    result = job_runner.execute_top20_job(window_minutes=60, top_k=20)
    print("MapReduce Result:")
    print(json.dumps(result, indent=2))
