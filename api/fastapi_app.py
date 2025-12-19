import random
import asyncio
import json
from datetime import datetime
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI(title="Dataco Event Generator")

# Sample categorical values (could be loaded from CSV for realism)
SHIPPING_MODES = ["Regular Air", "Express Air", "Delivery Truck"]
MARKETS = ["Europe", "US", "Asia", "Africa", "Canada"]
ORDER_REGIONS = ["Central", "West", "East", "South"]
CUSTOMER_SEGMENTS = ["Consumer", "Corporate", "Home Office"]
DEPARTMENT_IDS = list(range(1, 8))
CATEGORY_IDS = list(range(1, 12))
PRODUCT_CATEGORY_IDS = list(range(1, 20))

NUM_RANGE = {
    "Days for shipment (scheduled)": (1, 14),
    "Order Item Quantity": (1, 10),
    "Order Item Product Price": (5.0, 500.0),
    "Order Item Discount": (0.0, 50.0),
    "Latitude": (25.0, 55.0),
    "Longitude": (-120.0, -70.0)
}

HTML = """
<!DOCTYPE html>
<html>
  <head><title>WebSocket Test</title></head>
  <body>
    <h3>Streaming random Dataco-like events...</h3>
    <script>
      const ws = new WebSocket("ws://" + location.host + "/ws");
      ws.onmessage = (e) => console.log(e.data);
    </script>
  </body>
</html>
"""

@app.get("/")
async def root():
    return HTMLResponse(content=HTML)

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            now = datetime.utcnow()
            # numeric base fields
            days_sched = random.randint(*NUM_RANGE["Days for shipment (scheduled)"])
            qty = random.randint(*NUM_RANGE["Order Item Quantity"])
            price = round(random.uniform(*NUM_RANGE["Order Item Product Price"]), 2)
            discount = round(random.uniform(*NUM_RANGE["Order Item Discount"]), 2)
            discount_ratio = round(discount / price if price > 0 else 0.0, 4)
            # label unknown at streaming time; we predict it. Send placeholder -1
            payload = {
                "event_ts": now.isoformat(),
                "Shipping Mode": random.choice(SHIPPING_MODES),
                "Market": random.choice(MARKETS),
                "Order Region": random.choice(ORDER_REGIONS),
                "Customer Segment": random.choice(CUSTOMER_SEGMENTS),
                "Department Id": random.choice(DEPARTMENT_IDS),
                "Category Id": random.choice(CATEGORY_IDS),
                "Product Category Id": random.choice(PRODUCT_CATEGORY_IDS),
                "Days for shipment (scheduled)": days_sched,
                "Order Item Quantity": qty,
                "Order Item Product Price": price,
                "Order Item Discount": discount,
                "discount_ratio": discount_ratio,
                "Latitude": round(random.uniform(*NUM_RANGE["Latitude"]), 5),
                "Longitude": round(random.uniform(*NUM_RANGE["Longitude"]), 5)
            }
            await ws.send_text(json.dumps(payload))
            await asyncio.sleep(1.0)  # adjustable event frequency
    except WebSocketDisconnect:
        print("Client disconnected")
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        try:
            await ws.close()
        except RuntimeError:
            pass  # Already closed

# Run with: uvicorn api.fastapi_app:app --host 0.0.0.0 --port 8000
