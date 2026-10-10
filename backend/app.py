"""
FastAPI Server for Real-Time Purchase Trend Dashboard.
Exposes REST APIs, SSE real-time event streaming, and serves the frontend dashboard.
"""

import os
import sys
import json
import time
import asyncio
from typing import Optional
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.pipeline_coordinator import coordinator
from kafka_pipeline.broker_simulator import broker
from mapreduce.job_runner import job_runner
from hdfs_layer.hdfs_manager import hdfs_manager
from scheduler.refresh_daemon import scheduler
from dataset.products_catalog import PRODUCTS_CATALOG

app = FastAPI(title="Real-Time Purchase Trend Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize and seed pipeline on startup
@app.on_event("startup")
def startup_event():
    print("[Server] Initializing pipeline...")
    coordinator.initialize_and_seed()
    print("[Server] Pipeline is live and streaming!")

# Pydantic models for control requests
class ProducerRateRequest(BaseModel):
    rate: float

class ProducerSurgeRequest(BaseModel):
    product_id: int
    multiplier: float = 4.0
    duration_seconds: int = 45

class SchedulerIntervalRequest(BaseModel):
    interval_seconds: int

# REST Endpoints
@app.get("/api/dashboard/summary")
def get_dashboard_summary():
    return coordinator.get_top20_dashboard_data()

@app.get("/api/dashboard/top20")
def get_top20():
    data = coordinator.get_top20_dashboard_data()
    return {
        "timestamp": data["timestamp"],
        "top_products": data["top_products"],
        "metrics": data["metrics"]
    }

@app.get("/api/dashboard/trends")
def get_trends(top_n: int = 5):
    return coordinator.get_minute_trends(top_n=top_n)

@app.get("/api/dashboard/catalog")
def get_catalog():
    """Returns catalog of products available for promotion simulation."""
    return [{"product_id": p["product_id"], "product_name": p["product_name"], "department": p["department"]} for p in PRODUCTS_CATALOG]

@app.get("/api/hdfs/status")
def get_hdfs_status():
    return hdfs_manager.get_hdfs_metadata()

@app.get("/api/hdfs/files")
def get_hdfs_files(limit: int = 30):
    return hdfs_manager.list_files(limit=limit)

@app.get("/api/mapreduce/history")
def get_mapreduce_history():
    return {
        "job_history": job_runner.job_history,
        "last_job": job_runner.last_job_result
    }

@app.post("/api/pipeline/mapreduce/trigger")
def trigger_mapreduce():
    """Manual trigger for MapReduce aggregation."""
    result = scheduler.trigger_now()
    return {
        "status": "SUCCESS",
        "message": f"MapReduce Job {result['job_id']} executed successfully.",
        "job": result
    }

@app.post("/api/pipeline/producer/pause")
def pause_producer():
    coordinator.producer.pause()
    return {"status": "PAUSED"}

@app.post("/api/pipeline/producer/resume")
def resume_producer():
    coordinator.producer.resume()
    return {"status": "STREAMING"}

@app.post("/api/pipeline/producer/rate")
def set_producer_rate(req: ProducerRateRequest):
    coordinator.producer.set_rate(req.rate)
    return {"status": "UPDATED", "rate_per_sec": coordinator.producer.rate_per_sec}

@app.post("/api/pipeline/producer/surge")
def trigger_surge(req: ProducerSurgeRequest):
    coordinator.producer.trigger_promotion_surge(
        product_id=req.product_id,
        multiplier=req.multiplier,
        duration_seconds=req.duration_seconds
    )
    return {
        "status": "SURGE_ACTIVE",
        "product_id": req.product_id,
        "multiplier": req.multiplier,
        "duration_seconds": req.duration_seconds
    }

@app.post("/api/pipeline/scheduler/interval")
def set_interval(req: SchedulerIntervalRequest):
    scheduler.set_interval(req.interval_seconds)
    return {"status": "UPDATED", "interval_seconds": scheduler.interval_seconds}

# Real-Time Server-Sent Events (SSE) Stream
@app.get("/api/stream/events")
async def stream_live_events(request: Request):
    """
    Streams live transaction events from Kafka and periodic countdown updates to browser via SSE.
    """
    sub_q = broker.subscribe("instacart-transactions")

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                
                # Check for new Kafka transaction
                try:
                    # Non-blocking check
                    tx = sub_q.get_nowait()
                    data = json.dumps({"type": "TRANSACTION", "payload": tx})
                    yield f"data: {data}\n\n"
                except Exception:
                    pass

                # Emit pipeline heartbeat & countdown every second
                sched = scheduler.get_status()
                b_stats = broker.get_stats()
                heartbeat = json.dumps({
                    "type": "HEARTBEAT",
                    "countdown": sched["seconds_until_next_refresh"],
                    "interval": sched["interval_seconds"],
                    "tps": b_stats["current_rate_per_sec"],
                    "messages_total": b_stats["messages_published"]
                })
                yield f"data: {heartbeat}\n\n"

                await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass

    return StreamingResponse(event_generator(), media_type="text/event-stream")

# Serve frontend static files
frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend"))
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

@app.get("/")
def serve_dashboard():
    index_path = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Frontend loading...</h1>")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
