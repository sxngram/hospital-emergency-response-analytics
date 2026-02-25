from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE_DIR = Path(__file__).resolve().parents[1]
CURATED_DIR = BASE_DIR / "data" / "curated"
OUTPUT_DIR = BASE_DIR / "data" / "batch_output"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("HospitalEmergencyBatch")
        .master("local[*]")
        .getOrCreate()
    )

    fact = spark.read.parquet(str(CURATED_DIR / "fact_emergency_response.parquet"))
    hospitals = spark.read.parquet(str(CURATED_DIR / "dim_hospital.parquet"))

    daily_kpis = (
        fact.withColumn("event_date", F.to_date(F.col("date_key").cast("string"), "yyyyMMdd"))
        .groupBy("event_date", "hospital_key")
        .agg(
            F.count("incident_id").alias("incident_count"),
            F.avg("response_duration_minutes").alias("avg_response_duration_min"),
            F.avg("triage_minutes").alias("avg_triage_minutes"),
            F.sum(F.when(F.col("is_breach_sla") == True, 1).otherwise(0)).alias("sla_breach_count"),
            F.sum("cost_usd").alias("total_cost_usd"),
        )
        .join(hospitals.select("hospital_key", "hospital_name", "city"), on="hospital_key", how="left")
        .withColumn(
            "sla_breach_rate",
            F.round(F.col("sla_breach_count") / F.col("incident_count"), 4),
        )
    )

    city_window = Window.partitionBy("city").orderBy(F.desc("total_cost_usd"))
    windowed = daily_kpis.withColumn("cost_rank_in_city", F.dense_rank().over(city_window))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    windowed.coalesce(1).write.mode("overwrite").parquet(str(OUTPUT_DIR / "daily_kpis"))

    print(f"Batch output written to: {OUTPUT_DIR / 'daily_kpis'}")
    spark.stop()


if __name__ == "__main__":
    main()
