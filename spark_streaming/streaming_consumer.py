from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


TOPIC_NAME = "sensor-events"
BOOTSTRAP_SERVERS = "localhost:9092"


schema = StructType(
    [
        StructField("event_time", TimestampType(), True),
        StructField("sensor_id", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
    ]
)


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("KafkaSparkStreamingAnalytics")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )

    aggregated_stream = (
        parsed_stream
        .withWatermark("event_time", "1 minute")
        .groupBy(
            window(col("event_time"), "30 seconds"),
            col("sensor_id"),
        )
        .agg(
            count("*").alias("event_count"),
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("pressure").alias("avg_pressure"),
        )
    )

    query = (
        aggregated_stream
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
