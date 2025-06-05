from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from uuid import uuid4
import threading
import time
import queue

app = FastAPI()

# In-memory storage
ingestions = {}
batch_queue = queue.PriorityQueue()  # will sort by (priority, time)

priority_map = {"HIGH": 1, "MEDIUM": 2, "LOW": 3}

class IngestRequest(BaseModel):
    ids: List[int]
    priority: str

@app.post("/ingest")
def ingest(data: IngestRequest):
    ingestion_id = str(uuid4())
    ids = data.ids
    priority = data.priority
    created_time = time.time()

    batches = [ids[i:i+3] for i in range(0, len(ids), 3)]
    batch_infos = []

    for batch in batches:
        batch_id = str(uuid4())
        batch_infos.append({"batch_id": batch_id, "ids": batch, "status": "yet_to_start"})
        batch_queue.put((priority_map[priority], created_time, ingestion_id, batch_id, batch))

    ingestions[ingestion_id] = {
        "ingestion_id": ingestion_id,
        "status": "yet_to_start",
        "batches": batch_infos
    }

    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
def status(ingestion_id: str):
    ingestion = ingestions.get(ingestion_id)
    if not ingestion:
        return {"error": "Invalid ingestion_id"}

    batch_statuses = [b["status"] for b in ingestion["batches"]]

    if all(s == "yet_to_start" for s in batch_statuses):
        ingestion["status"] = "yet_to_start"
    elif all(s == "completed" for s in batch_statuses):
        ingestion["status"] = "completed"
    else:
        ingestion["status"] = "triggered"

    return ingestion

# Background batch processor
def worker():
    while True:
        if not batch_queue.empty():
            _, _, ingestion_id, batch_id, batch_ids = batch_queue.get()
            print(f"Processing batch {batch_id} with IDs {batch_ids}")
            ingestion = ingestions.get(ingestion_id)
            for b in ingestion["batches"]:
                if b["batch_id"] == batch_id:
                    b["status"] = "triggered"

            # Simulate processing delay
            time.sleep(5)
            for b in ingestion["batches"]:
                if b["batch_id"] == batch_id:
                    b["status"] = "completed"
        else:
            time.sleep(1)

# Start background worker thread
t = threading.Thread(target=worker, daemon=True)
t.start()
