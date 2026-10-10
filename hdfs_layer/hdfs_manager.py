"""
Hadoop HDFS Storage Layer for Instacart Transactions.
Stores ingested streaming batches into time-partitioned HDFS directories.
Provides WebHDFS compatibility, trailing-window retrieval, and block metadata.
"""

import os
import sys
import json
import time
import glob
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

class HDFSStorageManager:
    """
    Manages storage on HDFS.
    Supports native WebHDFS endpoints or partitioned HDFS layout on storage/hdfs/.
    """
    def __init__(self, base_dir: Optional[str] = None):
        if base_dir is None:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "storage", "hdfs"))
        self.base_dir = base_dir
        self.data_dir = os.path.join(self.base_dir, "data", "instacart_transactions")
        self.output_dir = os.path.join(self.base_dir, "output", "trailing_60min")
        
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        self.block_size = 134217728 # 128 MB default HDFS block size
        self.replication_factor = 3
        self.total_records_stored = 0
        self.total_bytes_stored = 0
        self.batch_counter = 0

    def get_partition_path(self, dt: datetime) -> str:
        """Partition by Year / Month / Day / Hour (standard Big Data partitioning)"""
        year_str = f"year={dt.strftime('%Y')}"
        month_str = f"month={dt.strftime('%m')}"
        day_str = f"day={dt.strftime('%d')}"
        hour_str = f"hour={dt.strftime('%H')}"
        part_dir = os.path.join(self.data_dir, year_str, month_str, day_str, hour_str)
        os.makedirs(part_dir, exist_ok=True)
        return part_dir

    def write_transaction_batch(self, transactions: List[Dict[str, Any]]) -> str:
        """
        Writes a micro-batch of transactions to HDFS partitioned storage.
        Format: JSON Lines (compatible with Hadoop TextInputFormat / MapReduce).
        """
        if not transactions:
            return ""

        self.batch_counter += 1
        now = datetime.now(timezone.utc)
        part_dir = self.get_partition_path(now)
        
        filename = f"part-{self.batch_counter:05d}-{int(now.timestamp())}.jsonl"
        filepath = os.path.join(part_dir, filename)

        serialized_lines = [json.dumps(tx) for tx in transactions]
        content = "\n".join(serialized_lines) + "\n"
        encoded = content.encode("utf-8")

        with open(filepath, "a", encoding="utf-8") as f:
            f.write(content)

        self.total_records_stored += len(transactions)
        self.total_bytes_stored += len(encoded)

        return filepath

    def get_trailing_records(self, minutes: int = 60) -> List[Dict[str, Any]]:
        """
        Retrieves all transaction records from HDFS that fall within the trailing N minutes.
        This provides the input split for the MapReduce job.
        """
        cutoff_epoch = time.time() - (minutes * 60)
        trailing_records = []

        # Find all jsonl files in data_dir
        pattern = os.path.join(self.data_dir, "**", "*.jsonl")
        files = glob.glob(pattern, recursive=True)

        for file in files:
            try:
                with open(file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        record = json.loads(line)
                        epoch = record.get("epoch_seconds")
                        if epoch is None and "timestamp" in record:
                            # Parse ISO timestamp
                            try:
                                dt = datetime.fromisoformat(record["timestamp"].replace("Z", "+00:00"))
                                epoch = dt.timestamp()
                            except Exception:
                                epoch = time.time()
                        
                        if epoch and epoch >= cutoff_epoch:
                            trailing_records.append(record)
            except Exception as e:
                print(f"[HDFS Manager] Warning reading file {file}: {e}")

        return trailing_records

    def write_mapreduce_output(self, top20_data: Dict[str, Any]) -> str:
        """Persists the MapReduce Top 20 aggregation result to HDFS output directory."""
        now = datetime.now(timezone.utc)
        filename = f"top20_{now.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(self.output_dir, filename)
        latest_filepath = os.path.join(self.output_dir, "top20_latest.json")

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(top20_data, f, indent=2)

        with open(latest_filepath, "w", encoding="utf-8") as f:
            json.dump(top20_data, f, indent=2)

        return latest_filepath

    def get_latest_mapreduce_output(self) -> Optional[Dict[str, Any]]:
        """Reads latest MapReduce output from HDFS."""
        latest_filepath = os.path.join(self.output_dir, "top20_latest.json")
        if os.path.exists(latest_filepath):
            try:
                with open(latest_filepath, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return None

    def get_hdfs_metadata(self) -> Dict[str, Any]:
        """Provides simulated WebHDFS cluster stats."""
        # Calculate actual directory disk usage
        total_size = 0
        file_count = 0
        for root, dirs, files in os.walk(self.base_dir):
            for f in files:
                file_count += 1
                try:
                    total_size += os.path.getsize(os.path.join(root, f))
                except Exception:
                    pass

        blocks_allocated = max(1, (total_size // self.block_size) + 1)

        return {
            "cluster_id": "CID-bda-instacart-hdfs-01",
            "namenode": "hdfs://namenode:9000",
            "hdfs_uri": "/data/instacart_transactions",
            "configured_capacity_gb": 500.0,
            "used_capacity_mb": round(total_size / (1024 * 1024), 2),
            "total_files": file_count,
            "total_records_stored": self.total_records_stored,
            "block_size_mb": 128,
            "replication_factor": self.replication_factor,
            "blocks_allocated": blocks_allocated,
            "health": "HEALTHY_ACTIVE",
            "under_replicated_blocks": 0
        }

    def list_files(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Lists files stored in HDFS partition paths."""
        results = []
        pattern = os.path.join(self.data_dir, "**", "*.jsonl")
        files = glob.glob(pattern, recursive=True)
        # Sort by modification time descending
        files.sort(key=os.path.getmtime, reverse=True)

        for file in files[:limit]:
            rel_path = os.path.relpath(file, self.base_dir).replace("\\", "/")
            stat = os.stat(file)
            results.append({
                "path": f"hdfs://cluster/{rel_path}",
                "size_bytes": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                "replication": self.replication_factor,
                "owner": "hdfs",
                "group": "supergroup",
                "permission": "rw-r--r--"
            })
        return results

# Shared HDFS manager instance
hdfs_manager = HDFSStorageManager()
