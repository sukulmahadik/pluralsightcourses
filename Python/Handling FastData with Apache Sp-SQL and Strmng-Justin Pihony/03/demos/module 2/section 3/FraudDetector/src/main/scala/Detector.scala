package main

import org.apache.spark.sql.{SaveMode, SparkSession}

object Detector {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
                            .master("local")
                            .appName("Fraud Detector")
                            .config("spark.driver.memory", "2g")
				                    .enableHiveSupport
                            .getOrCreate()
				  
	  import spark.implicits._	  
	  spark.read.json("Data/finances-small.json")
	       .na.drop("all")
         .na.fill("Unknown", Seq("Description"))
         .where(($"Amount" !== 0) || $"Description" === "Unknown")
         .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")
  }
}