from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("Monitoring Gudang Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Skema data sensor
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Baca stream suhu
suhu_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .option("startingOffsets", "latest") \
    .load()

suhu_parsed = suhu_df.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_suhu).alias("data"), "timestamp") \
    .select("data.*", "timestamp")

# Baca stream kelembaban
kelembaban_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .option("startingOffsets", "latest") \
    .load()

kelembaban_parsed = kelembaban_df.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_kelembaban).alias("data"), "timestamp") \
    .select("data.*", "timestamp")

# Filter suhu > 80
peringatan_suhu = suhu_parsed.filter(col("suhu") > 80)
peringatan_kelembaban = kelembaban_parsed.filter(col("kelembaban") > 70)

# Join 2 stream berdasarkan gudang_id dan window waktu
suhu_with_watermark = suhu_parsed.withWatermark("timestamp", "15 seconds")
kelembaban_with_watermark = kelembaban_parsed.withWatermark("timestamp", "15 seconds")

joined = suhu_with_watermark.join(
    kelembaban_with_watermark,
    (suhu_with_watermark["gudang_id"] == kelembaban_with_watermark["gudang_id"]) &
    (suhu_with_watermark["timestamp"] >= kelembaban_with_watermark["timestamp"] - expr("interval 10 seconds")) &
    (suhu_with_watermark["timestamp"] <= kelembaban_with_watermark["timestamp"] + expr("interval 10 seconds")),
    "inner"
)

# Tambahkan status
hasil = joined.select(
    suhu_with_watermark["gudang_id"].alias("gudang_id"),
    suhu_with_watermark["suhu"],
    kelembaban_with_watermark["kelembaban"]
).withColumn(
    "status",
    expr("""
        CASE
            WHEN suhu > 80 AND kelembaban > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
            WHEN suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
            WHEN kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
            ELSE 'Aman'
        END
    """)
)

# Output ke console
query = hasil.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
