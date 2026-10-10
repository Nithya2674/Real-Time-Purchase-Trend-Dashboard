#!/usr/bin/env python3
"""
Master Pipeline Runner - Problem 21: RealTime Purchase Trend Dashboard.
Starts the complete Big Data Analytics pipeline:
  - Instacart Transaction Streaming Engine
  - Apache Kafka Message Broker & Producer
  - Hadoop HDFS Storage Sink & Partition Manager
  - Hadoop MapReduce Trailing 60-Minute Engine
  - 5-Minute Scheduler Daemon
  - Live Executive Dashboard Web Application
"""

import os
import sys
import time
import uvicorn

# Ensure root directory is in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

def print_banner():
    banner = r"""
  ========================================================================
   REAL-TIME PURCHASE TREND DASHBOARD (PROBLEM 21 - BDA)
   Tech Stack: Apache Kafka | Hadoop HDFS | MapReduce | Live Dashboard
   Dataset: Instacart Market Basket Analysis
  ========================================================================
  [+] Pipeline Architecture:
      Instacart Source ---> Kafka Topic ---> HDFS Sink ---> MapReduce (60m) ---> Live Web Dashboard
  
  [+] Scheduled Cycle: Every 5 Minutes (300 seconds)
  [+] Trailing Analysis Window: Trailing 60 Minutes Sliding Window
  [+] Output Target: Top 20 Products by Units Sold with Velocity Shift Indicators
  
  >> DASHBOARD URL: http://localhost:8000
  >> API DOCS URL:  http://localhost:8000/docs
  ========================================================================
    """
    print(banner)

def main():
    print_banner()
    print("[*] Launching FastAPI Web Application and Streaming Pipeline...")
    uvicorn.run("backend.app:app", host="0.0.0.0", port=8000, log_level="info", reload=False)

if __name__ == "__main__":
    main()
