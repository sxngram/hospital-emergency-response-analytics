from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parents[1]
STREAM_INPUT = BASE_DIR / "data" / "stream_input"
STREAM_OUTPUT = BASE_DIR / "data" / "stream_output"
CHECKPOINT = BASE_DIR / "data" / "stream_checkpoint"


def main() -> None:
    STREAM_INPUT.mkdir(parents=True, exist_ok=True)
    STREAM_OUTPUT.mkdir(parents=True, exist_ok=True)
    CHECKPOINT.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("HospitalEmergencyStreaming")
        .master("local[*]")
        .getOrCreate()
    )

    schema = """
        incident_id LONG,
        incident_ts STRING,
        hospital_id STRING,
        incident_type STRING,
        severity INT,
        patient_count INT,
        response_duration_minutes INT,
        triage_minutes INT,
        cost_usd DOUBLE
    """

    stream_df = (
        spark.readStream
        .schema(schema)
        .option("header", True)
        .csv(str(STREAM_INPUT))
        .withColumn("event_ts", F.to_timestamp("incident_ts"))
        .withWatermark("event_ts", "10 minutes")
    )

    aggregated = (
        stream_df.groupBy(
            F.window("event_ts", "15 minutes"),
            F.col("hospital_id"),
            F.col("incident_type"),
        )
        .agg(
            F.count("incident_id").alias("incident_count"),
            F.avg("response_duration_minutes").alias("avg_response_duration_min"),
            F.avg("triage_minutes").alias("avg_triage_minutes"),
            F.sum("cost_usd").alias("total_cost_usd"),
        )
    )

    query = (
        aggregated.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", str(STREAM_OUTPUT))
        .option("checkpointLocation", str(CHECKPOINT))
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("Streaming query started. Drop csv files into data/stream_input/ to process events.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
