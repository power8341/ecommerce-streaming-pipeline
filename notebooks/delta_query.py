from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, round, desc

spark = SparkSession.builder \
    .appName("QueryDelta") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_4.1_2.13:4.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/tmp/delta/ecommerce_orders")

print(f"\nTotal orders captured: {df.count()}")
print("\nSample orders:")
df.show(5)

print("\nOrders by category:")
df.groupBy("category").agg(count("order_id").alias("orders")).orderBy(desc("orders")).show()

print("\nRevenue by category:")
df.groupBy("category").agg(round(sum("total_amount"), 2).alias("total_revenue")).orderBy(desc("total_revenue")).show()
