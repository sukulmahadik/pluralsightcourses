package main

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Detector {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
                            .master("local")
                            .appName("Fraud Detector")
                            .config("spark.driver.memory", "2g")
				                    .enableHiveSupport
                            .getOrCreate()
				  
	  import spark.implicits._
	  val financesDF = spark.read.json("Data/finances-small.json")
	  
	  val accountNumberPrevious4WindowSpec = Window.partitionBy($"AccountNumber")
	                                               .orderBy($"Date").rowsBetween(-4,0)
	  val rollingAvgForPrevious4PerAccount = avg($"Amount").over(accountNumberPrevious4WindowSpec)
	  
	  financesDF
	            .na.drop("all", Seq("ID","Account","Amount","Description","Date"))
              .na.fill("Unknown", Seq("Description"))
              .where($"Amount" =!= 0 || $"Description" === "Unknown")
              .selectExpr("Account.Number as AccountNumber", "Amount", 
                          "to_date(CAST(unix_timestamp(Date, 'MM/dd/yyyy') AS TIMESTAMP)) AS Date", 
                          "Description")
              .withColumn("RollingAverage", rollingAvgForPrevious4PerAccount)
              .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")
              
    if(financesDF.hasColumn("_corrupt_record")) {
	    financesDF.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
	              .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
    }
    
    financesDF
	            .select(concat($"Account.FirstName", lit(" "), $"Account.LastName").as("FullName"), 
	                    $"Account.Number".as("AccountNumber"))
			        .distinct
              .coalesce(5)
			        .write.mode(SaveMode.Overwrite).json("Output/finances-small-accounts")
			        
	  financesDF
              .select($"Account.Number".as("AccountNumber"), $"Amount", $"Description",
                      to_date(unix_timestamp($"Date","MM/dd/yyyy").cast("timestamp")).as("Date"))
              .groupBy($"AccountNumber")
              .agg(avg($"Amount").as("AverageTransaction"), sum($"Amount").as("TotalTransactions"),
                   count($"Amount").as("NumberOfTransactions"), max($"Amount").as("MaxTransaction"),
                   min($"Amount").as("MinTransaction"), stddev($"Amount").as("StandardDeviationAmount"),
                   collect_set($"Description").as("UniqueTransactionDescriptions")) 
              .coalesce(5)
              .write.mode(SaveMode.Overwrite).json("Output/finances-small-account-details")
  }
  
  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try //org.apache.spark.sql.AnalysisException
    def hasColumn(columnName: String) = Try(df(columnName)).isSuccess
  }
}