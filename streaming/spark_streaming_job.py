import os
import json
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType, TimestampType)
from pyspark.ml import PipelineModel

MODEL_DIR = os.environ.get("MODEL_DIR", "/src/models")
MODEL_PATH = os.path.join(MODEL_DIR, "late_delivery_pipeline_safe")
SOCKET_HOST = os.environ.get("BRIDGE_HOST", "bridge")
SOCKET_PORT = int(os.environ.get("BRIDGE_TCP_PORT", "9999"))
POSTGRES_URL = os.environ.get("POSTGRES_URL", "jdbc:postgresql://postgres:5432/logistics")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "loguser")
POSTGRES_PASS = os.environ.get("POSTGRES_PASSWORD", "logpass")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "logistics")
MONGO_COLLECTION_AGG = os.environ.get("MONGO_COLLECTION_AGG", "aggregates")

spark = (SparkSession.builder
         .appName("late-delivery-stream")
         .config("spark.sql.shuffle.partitions", "64")
         .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

print(f"Loading pipeline model from: {MODEL_PATH}")
model = PipelineModel.load(MODEL_PATH)

schema = StructType([
    StructField("event_ts", StringType(), True),
    StructField("Shipping Mode", StringType(), True),
    StructField("Market", StringType(), True),
    StructField("Order Region", StringType(), True),
    StructField("Customer Segment", StringType(), True),
    StructField("Department Id", IntegerType(), True),
    StructField("Category Id", IntegerType(), True),
    StructField("Product Category Id", IntegerType(), True),
    StructField("Days for shipment (scheduled)", IntegerType(), True),
    StructField("Order Item Quantity", IntegerType(), True),
    StructField("Order Item Product Price", DoubleType(), True),
    StructField("Order Item Discount", DoubleType(), True),
    StructField("discount_ratio", DoubleType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True)
])

raw_stream = (spark.readStream
              .format("socket")
              .option("host", SOCKET_HOST)
              .option("port", SOCKET_PORT)
              .load())

# Parse JSON lines to columns
json_df = raw_stream.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")

# Convert event_ts to timestamp and derive time parts
with_ts = (json_df
           .withColumn("order_ts", F.to_timestamp("event_ts"))
           .withColumn("order_hour", F.hour("order_ts"))
           .withColumn("order_dow", F.dayofweek("order_ts"))
           .withColumn("order_month", F.month("order_ts"))
           .withColumn("label", F.lit(0))  # placeholder; model predicts late risk
          )

# Select columns expected by batch pipeline (safe feature set)
feature_cols = [
    "label",
    "Shipping Mode", "Market", "Order Region", "Customer Segment",
    "Department Id", "Category Id", "Product Category Id",
    "Days for shipment (scheduled)", "Order Item Quantity", "Order Item Product Price",
    "Order Item Discount", "discount_ratio", "Latitude", "Longitude",
    "order_hour", "order_dow", "order_month"
]

# Include event_ts in the selection so it survives the model transform
features_ready = with_ts.select(*(feature_cols + ["event_ts"]))

# Apply model transform (will add prediction columns)
pred_stream = model.transform(features_ready)

# Extract probability of late (assuming index 1 corresponds to positive class)
# Use UDF to extract from Vector
@F.udf(DoubleType())
def get_late_probability(v):
    try:
        return float(v[1])
    except:
        return 0.0

prob_col = get_late_probability(F.col("probability")).alias("late_prob")
# result_cols must include event_ts for the aggregation step
result_cols = feature_cols + ["prediction", prob_col, "event_ts"]
final_stream = pred_stream.select(*result_cols)

# Write predictions to Postgres each batch
def write_predictions(batch_df, batch_id):
    try:
        (batch_df
         .withColumn("ingest_batch_id", F.lit(batch_id))
         .write
         .format("jdbc")
         .option("url", POSTGRES_URL)
         .option("driver", "org.postgresql.Driver")
         .option("user", POSTGRES_USER)
         .option("password", POSTGRES_PASS)
         .option("dbtable", "stream_predictions")
         .mode("append")
         .save())
    except Exception as e:
        print(f"Error writing batch {batch_id} to Postgres: {e}")
        # raise e
    # batch_df.show()

# Windowed aggregation (1-minute) average late probability by Market
agg_stream = (final_stream
              .withColumn("order_ts", F.to_timestamp("event_ts"))
              .groupBy(F.window("order_ts", "1 minute"), F.col("Market"))
              .agg(F.avg("late_prob").alias("avg_late_prob"), F.count("late_prob").alias("events")))

from pymongo import MongoClient
client = MongoClient(MONGO_URI)
mdb = client[MONGO_DB]
collection = mdb[MONGO_COLLECTION_AGG]

def write_aggregates(batch_df, batch_id):
    try:
        rows = batch_df.collect()
        docs = []
        for r in rows:
            window = r["window"]
            docs.append({
                "batch_id": batch_id,
                "window_start": window.start.isoformat(),
                "window_end": window.end.isoformat(),
                "Market": r["Market"],
                "avg_late_prob": float(r["avg_late_prob"]),
                "events": int(r["events"])
            })
        if docs:
            collection.insert_many(docs)
            print(f"Inserted {len(docs)} aggregate docs (batch {batch_id})")
    except Exception as e:
        print(f"Error writing aggregates batch {batch_id}: {e}")

pred_query = (final_stream.writeStream
              .queryName("prediction_query")
              .outputMode("append")
              .foreachBatch(write_predictions)
              .option("checkpointLocation", "/tmp/chk_preds")
              .start())

agg_query = (agg_stream.writeStream
             .queryName("aggregation_query")
             .outputMode("update")
             .foreachBatch(write_aggregates)
             .option("checkpointLocation", "/tmp/chk_aggs")
             .start())

spark.streams.awaitAnyTermination()

print("Streaming job started. Awaiting termination...")

spark.streams.awaitAnyTermination()
