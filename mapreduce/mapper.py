#!/usr/bin/env python3
"""
Hadoop Streaming Mapper for Trailing 60-Minute Top Products.
Reads transaction records from STDIN.
Filters records within the trailing 60 minutes window.
Emits: product_id \t quantity \t product_name \t department \t aisle \t total_price
"""

import sys
import json
import time

def run_mapper(window_minutes=60, current_epoch=None):
    if current_epoch is None:
        current_epoch = time.time()
    cutoff_epoch = current_epoch - (window_minutes * 60)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            record = json.loads(line)
            epoch = record.get("epoch_seconds")
            
            # Trailing 60 minutes filter
            if epoch is not None and epoch < cutoff_epoch:
                continue

            product_id = record.get("product_id")
            if not product_id:
                continue

            quantity = int(record.get("quantity", 1))
            product_name = record.get("product_name", "Unknown Product")
            department = record.get("department", "other")
            aisle = record.get("aisle", "other")
            total_price = float(record.get("total_price", 0.0))

            # Emit TSV format: key \t val1 \t val2 \t val3 \t val4 \t val5
            print(f"{product_id}\t{quantity}\t{product_name}\t{department}\t{aisle}\t{total_price}")

        except Exception:
            # Skip corrupted input records
            continue

if __name__ == "__main__":
    # Support optional window_minutes argument: python mapper.py 60
    window = 60
    if len(sys.argv) > 1:
        try:
            window = int(sys.argv[1])
        except ValueError:
            pass
    run_mapper(window_minutes=window)
