"""
Helper to load or parse real Instacart Market Basket Analysis CSV files if downloaded from Kaggle.
Kaggle dataset URL: https://www.kaggle.com/c/instacart-market-basket-analysis/data
"""

import os
import csv
import json
from typing import Generator, Dict, Any

RAW_DATA_DIR = os.path.join(os.path.dirname(__file__), "raw")

def has_raw_kaggle_data() -> bool:
    """Checks if Kaggle CSV files are placed in dataset/raw/ directory."""
    products_file = os.path.join(RAW_DATA_DIR, "products.csv")
    orders_file = os.path.join(RAW_DATA_DIR, "orders.csv")
    order_products = os.path.join(RAW_DATA_DIR, "order_products__prior.csv")
    return os.path.exists(products_file) and (os.path.exists(orders_file) or os.path.exists(order_products))

def print_kaggle_instructions():
    print("=" * 70)
    print("INSTACART MARKET BASKET ANALYSIS DATASET SETUP")
    print("=" * 70)
    print("1. Download the Instacart dataset from:")
    print("   https://www.kaggle.com/c/instacart-market-basket-analysis/data")
    print(f"2. Extract the following CSV files into '{RAW_DATA_DIR}':")
    print("   - products.csv")
    print("   - aisles.csv")
    print("   - departments.csv")
    print("   - order_products__prior.csv (or order_products__train.csv)")
    print("   - orders.csv")
    print("3. When present, the streaming Kafka producer will stream actual recorded")
    print("   orders from the Kaggle dataset directly into the Kafka topic!")
    print("=" * 70)

if __name__ == "__main__":
    if not has_raw_kaggle_data():
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        print_kaggle_instructions()
    else:
        print(f"Detected raw Kaggle Instacart CSVs in {RAW_DATA_DIR}!")
