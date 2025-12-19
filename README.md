# Système Prédictif Intelligent de Gestion Logistique
## Streaming / Real-Time Pipeline (Spark Structured Streaming)

This project now includes a real-time component that ingests synthetic Dataco-like order events via FastAPI + WebSocket, bridges them onto a TCP socket for Spark Structured Streaming, applies the previously trained leakage-safe RandomForest pipeline, stores predictions in Postgres, aggregates windowed metrics into MongoDB, and visualizes them with Streamlit. A lightweight Airflow DAG skeleton is provided for orchestration.

### Architecture Overview

```
FastAPI (WebSocket) --> Bridge (WebSocket client -> TCP server) --> Spark Structured Streaming
															  |-> Prediction sink: Postgres (table: stream_predictions)
															  |-> Aggregation sink: MongoDB (collection: aggregates)
Aggregates + Predictions --> Streamlit Dashboard
Airflow DAG (optional) orchestrates health checks and spark-submit
```

### Components

- `api/fastapi_app.py`: Generates random JSON events every second matching the safe feature schema.
- `streaming/bridge.py`: Connects to FastAPI WebSocket; serves newline-delimited JSON over TCP port `9999` for Spark.
- `streaming/spark_streaming_job.py`: Reads socket, parses JSON, derives time-part features, loads pipeline model from `MODEL_DIR`, outputs predictions to Postgres and windowed aggregates to MongoDB.
- `streamlit_dash/dashboard_streaming.py`: Live dashboard of aggregates (trend + latest window + market bar chart).
- `airflow/dags/streaming_pipeline_dag.py`: Skeleton DAG with health checks and a spark-submit placeholder.

### Model Dependency
Ensure the batch-trained model directory `late_delivery_pipeline_safe` exists under `./src/models` before starting the streaming stack. Re-run notebook Cells 5–13 if necessary.

### Running the Stack

1. Build images:
```powershell
docker compose build
```
2. Start services (omit `spark-stream` initially to allow bridge connection ready):
```powershell
docker compose up -d postgres mongo fastapi bridge spark-stream streamlit airflow
```
3. Verify FastAPI events:
```powershell
curl http://localhost:8000
```
4. Check MongoDB aggregates (after ~1-2 minutes):
```powershell
docker exec -it mongo mongosh --eval "db.logistics.aggregates.find().limit(5)"
```
5. Open Streamlit dashboard for batch (existing) and optionally add a new one for streaming dashboard by running:
```powershell
docker exec -it streamlit-app python /src/streamlit_dash/dashboard_streaming.py
```
If you want a separate container UI, add another service or adapt existing `streamlit` command.

### Postgres Table Creation (Optional Explicit)
The streaming job will append automatically. To inspect:
```powershell
docker exec -it postgres psql -U loguser -d logistics -c "\d stream_predictions"
docker exec -it postgres psql -U loguser -d logistics -c "SELECT * FROM stream_predictions ORDER BY ingest_batch_id DESC LIMIT 10;"
```

### Airflow Usage
Access Airflow UI at `http://localhost:8080` (user: admin / password: admin). Trigger the `streaming_pipeline` DAG manually or adjust the schedule. The current DAG only performs basic health checks and a spark-submit placeholder.

### Environment Variables
Key variables defined in `docker-compose.yml`:
- MODEL_DIR: Path inside containers for pipeline model (`/src/models`).
- BRIDGE_TCP_PORT: TCP port for socket source (9999).
- POSTGRES_*: Connection info for prediction sink.
- MONGO_*: Connection info for aggregate sink & dashboard.

### Extending / Hardening
- Replace simple bridge with Kafka for scalability.
- Add authentication/SSL for FastAPI and databases.
- Implement proper schema evolution and error handling in streaming job.
- Add Airflow tasks for archiving raw events, cleaning checkpoints, and monitoring lag.

### Troubleshooting
- If Spark cannot connect: ensure `bridge` container is healthy and port 9999 exposed internally.
- If model load fails: verify `late_delivery_pipeline_safe` directory exists in mounted `./src/models` host path.
- If Mongo aggregates empty: wait a minute; ensure events flowing (check FastAPI logs) and streaming queries started.
- Memory pressure: reduce shuffle partitions or increase container memory limits; adjust RF complexity in batch model.
