// Databricks notebook source

// Configuration for Kafka brokers
val kafkaBrokers = "10.0.0.x:9092,10.0.0.x:9092,10.0.0.x:9092"
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Setup connection to Kafka
val kafka = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBrokers)
  .option("subscribe", "tweets")
  .option("startingOffsets", "earliest")
  .load()

val kafkaData = kafka
  .withColumn("Key", $"key".cast(StringType))
  .withColumn("Topic", $"topic".cast(StringType))
  .withColumn("Offset", $"offset".cast(LongType))
  .withColumn("Partition", $"partition".cast(IntegerType))
  .withColumn("Timestamp", $"timestamp".cast(TimestampType))
  .withColumn("Value", $"value".cast(StringType))
  .select("Key", "Value", "Partition", "Offset", "Timestamp")

kafkaData.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()