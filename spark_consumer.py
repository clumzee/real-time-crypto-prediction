from distutils.debug import DEBUG
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import environ

import time

import os

spark_version = '3.3.0'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[3] pyspark-shell'

env = environ.Env(DEBUG=(bool, False))

environ.Env.read_env()


kafka_topic_name = env("KAFKA_TOPIC_NAME")
kafka_bootstrap_servers = env("BOOTSTRAP_SERVERS")

if __name__ == "__main__":

    spark = (
        SparkSession.builder.appName("spark session").master("local[*]").getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    bitcoin_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic_name)
        .option("startingOffsets", "latest")
        .load()
    )

    print("Printing Schema of orders_df: ")
    bitcoin_df.printSchema()

    bitcoin_df1 = bitcoin_df.selectExpr("CAST(value AS STRING)", "timestamp")

    bitcoin_schema = (
        StructType()
        .add("time_exchange", TimestampType())
        .add("time_coinapi", TimestampType())
        .add("uuid", StringType())
        .add("price", FloatType())
        .add("size", FloatType())
        .add("taker_side", StringType())
        .add("symbol_id", StringType())
        .add("sequence", IntegerType())
        .add("type", StringType())
    )

    bitcoin_df2 = bitcoin_df1.select(
        from_json(col("value"), bitcoin_schema).alias("Bitcoins"), "timestamp"
    )

    bitcoin_df3 = (
        bitcoin_df2.groupBy("time_exchange")
        .agg({"price": "mean"})
        .select(col("mean(price)").alias("Price"))
    )

    bitcoin_df3.printSchema()

    bitcoin_agg_write_stream = (
        bitcoin_df3.writeStream.trigger(processingTime="5 seconds")
        .outputMode("update")
        .option("truncate", "false")
        .format("console")
        .start()
    )

    bitcoin_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")
