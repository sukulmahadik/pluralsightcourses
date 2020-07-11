package main

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

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
	  financesDF
	            .na.drop("all", Seq("ID","Account","Amount","Description","Date"))
              .na.fill("Unknown", Seq("Description"))
              .where($"Amount" =!= 0 || $"Description" === "Unknown")
              .selectExpr( "Account.Number as AccountNumber" , "Amount" , "Date" , "Description")
              .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")
              
    if(financesDF.hasColumn("_corrupt_record")) {
	    financesDF.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
	              .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
    }
    
    financesDF
	            .select(concat($"Account.FirstName", lit(" "), $"Account.LastName").as("FullName"), 
	                    $"Account.Number".as("AccountNumber"))
			        .distinct
			        .write.mode(SaveMode.Overwrite).json("Output/finances-small-accounts")
  }
  
  implicit class DataFrameHelper(df: DataFrame) {
    import scala.util.Try //org.apache.spark.sql.AnalysisException
    def hasColumn(columnName: String) = Try(df(columnName)).isSuccess
  }
}