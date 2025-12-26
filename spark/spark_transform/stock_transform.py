import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, arrays_zip, from_unixtime
from pyspark.sql.types import DateType, StructType, StructField, ArrayType, LongType, StringType

if __name__ == "__main__":
    # ------------------------------------------------------------------
    # Spark Session
    # ------------------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("StockProcessing")
        .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://minio:9000"))
        .config("fs.s3a.path.style.access", "true")
        .config("fs.s3a.connection.ssl.enabled", "false")
        .config("fs.s3a.attempts.maximum", "1")
        .config("fs.s3a.connection.establish.timeout", "5000")
        .config("fs.s3a.connection.timeout", "10000")
        .getOrCreate()
    )

    # ------------------------------------------------------------------
    # Input
    # ------------------------------------------------------------------
    print("Reading stock prices from MinIO...")

    input_path = os.getenv("SPARK_APPLICATION_ARGS")
    if not input_path:
        raise ValueError("SPARK_APPLICATION_ARGS is not set")

    print("INPUT PATH:", input_path)

    input_file = f"s3a://{input_path}/_stock_data.json"

    schema = StructType([
        StructField("meta", StructType([
            StructField("symbol", StringType(), True),
        ]), True),
        StructField("timestamp", ArrayType(LongType())),
        StructField("indicators", StructType([
            StructField("quote", ArrayType(StructType([
                StructField("close", ArrayType(StringType()), True),
                StructField("high", ArrayType(StringType()), True),
                StructField("low", ArrayType(StringType()), True),
                StructField("open", ArrayType(StringType()), True),
                StructField("volume", ArrayType(StringType()), True),
            ])), True),

        ]), True),
    ]
    )

    df = (spark.read
          .schema(schema=schema)
          .option("multiline", "true")
          .json(f"{input_file}"))
    print(df.printSchema())

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------
    df_exploded = (
        df.select("timestamp", explode("indicators.quote").alias("quote"))
        .select("timestamp", "quote.*")
    )
    print(df.show(10))

    df_zipped = (
        df_exploded
        .select(arrays_zip("timestamp", "close", "high", "low", "open", "volume").alias("zipped"))
        .select(explode("zipped").alias("row"))
        .select("row.timestamp", "row.close", "row.high", "row.low", "row.open", "row.volume" )
        .withColumn("date", from_unixtime("timestamp").cast(DateType()))
    )

    df_zipped.show(truncate=False)

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------
    output_path = f"s3a://{input_path}/stock_formatted_prices"
    print("Writing formatted data to:", output_path)

    (
        df_zipped.write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )

    print(f"Data written successfully to {output_path}")

    spark.stop()
