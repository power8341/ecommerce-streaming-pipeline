from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("EcommerceStreamingPipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
            "io.delta:delta-spark_4.1_2.13:4.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_email", StringType(), True),
    StructField("customer_country", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("unit_price", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
])

print("Connecting to Kafka...")

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce-orders") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

delta_path = "/tmp/delta/ecommerce_orders"
checkpoint_path = "/tmp/checkpoint/ecommerce_orders"

print("Starting stream... writing to Delta Lake")

query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .option("path", delta_path) \
    .trigger(processingTime="5 seconds") \
    .start()

print("Stream is running! Orders are being written to Delta Lake every 5 seconds.")
print("Press Ctrl+C to stop.")

query.awaitTermination()
