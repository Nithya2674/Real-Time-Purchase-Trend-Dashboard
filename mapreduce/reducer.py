#!/usr/bin/env python3
"""
Hadoop Streaming Reducer for Trailing 60-Minute Top 20 Products.
Reads sorted (product_id, quantity, ...) pairs from STDIN.
Aggregates units sold and total revenue per product.
Emits the top 20 products ranked by units sold in the trailing window.
"""

import sys
import json
import time

def run_reducer(top_n=20):
    current_product_id = None
    current_units = 0
    current_revenue = 0.0
    current_name = ""
    current_dept = ""
    current_aisle = ""

    aggregated_products = []
    total_window_units = 0
    total_window_revenue = 0.0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) < 6:
            continue

        product_id = parts[0]
        try:
            quantity = int(parts[1])
            product_name = parts[2]
            department = parts[3]
            aisle = parts[4]
            total_price = float(parts[5])
        except (ValueError, IndexError):
            continue

        if current_product_id == product_id:
            current_units += quantity
            current_revenue += total_price
        else:
            if current_product_id is not None:
                aggregated_products.append({
                    "product_id": int(current_product_id),
                    "product_name": current_name,
                    "department": current_dept,
                    "aisle": current_aisle,
                    "units_sold": current_units,
                    "revenue": round(current_revenue, 2)
                })
                total_window_units += current_units
                total_window_revenue += current_revenue

            current_product_id = product_id
            current_units = quantity
            current_revenue = total_price
            current_name = product_name
            current_dept = department
            current_aisle = aisle

    # Don't forget last product
    if current_product_id is not None:
        aggregated_products.append({
            "product_id": int(current_product_id),
            "product_name": current_name,
            "department": current_dept,
            "aisle": current_aisle,
            "units_sold": current_units,
            "revenue": round(current_revenue, 2)
        })
        total_window_units += current_units
        total_window_revenue += current_revenue

    # Sort descending by units_sold, then revenue
    aggregated_products.sort(key=lambda x: (x["units_sold"], x["revenue"]), reverse=True)

    top_products = aggregated_products[:top_n]

    # Calculate ranks and percentages
    for rank, p in enumerate(top_products, start=1):
        p["rank"] = rank
        p["share_pct"] = round((p["units_sold"] / max(1, total_window_units)) * 100, 2)
        p["avg_price"] = round(p["revenue"] / max(1, p["units_sold"]), 2)

    result = {
        "job_id": f"job_{int(time.time()*1000)}_0001",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "trailing_window_minutes": 60,
        "total_units_sold": total_window_units,
        "total_revenue": round(total_window_revenue, 2),
        "unique_products_active": len(aggregated_products),
        "top_n_count": len(top_products),
        "top_products": top_products
    }

    # Print JSON output to STDOUT
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    top_k = 20
    if len(sys.argv) > 1:
        try:
            top_k = int(sys.argv[1])
        except ValueError:
            pass
    run_reducer(top_n=top_k)
