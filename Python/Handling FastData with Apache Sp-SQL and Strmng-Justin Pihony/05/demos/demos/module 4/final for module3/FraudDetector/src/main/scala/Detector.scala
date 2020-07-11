package main

import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.cassandra._

case class Account(number: String, firstName: String, lastName: String)
case class Transaction(id: Long, account: Account, date: java.sql.Date, amount: Double, 
                       description: String)
case class TransactionForAverage(accountNumber: String, amount: Double, description: String, 
                                 date: java.sql.Date)

object Detector {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
                            .master("local")
                            .appName("Fraud Detector")
                            .config("spark.driver.memory", "2g")
                            .config("spark.cassandra.connection.host", "localhost")
				                    .enableHiveSupport
                            .getOrCreate()
				  
	  import spark.implicits._
	  val financesDS = spark.read.json("Data/finances-small.json")
	                        .withColumn("date", to_date(unix_timestamp($"Date","MM/dd/yyyy")
	                                                   .cast("timestamp"))).as[Transaction]
	  
	  val accountNumberPrevious4WindowSpec = Window.partitionBy($"AccountNumber")
	                                               .orderBy($"Date").rowsBetween(-4,0)
	  val rollingAvgForPrevious4PerAccount = avg($"Amount").over(accountNumberPrevious4WindowSpec)
	  
	  financesDS
	            .na.drop("all", Seq("ID","Account","Amount","Description","Date"))
              .na.fill("Unknown", Seq("Description")).as[Transaction]
              //.filter(tx=>(tx.amount != 0 || tx.description == "Unknown"))
              .where($"Amount" =!= 0 || $"Description" === "Unknown")
              .select($"Account.Number".as("AccountNumber").as[String], $"Amount".as[Double], 
                      $"Date".as[java.sql.Date](Encoders.DATE), $"Description".as[String])
              .withColumn("RollingAverage", rollingAvgForPrevious4PerAccount)
              .write.mode(SaveMode.Overwrite).parquet("Output/finances-small")
              
    if(financesDS.hasColumn("_corrupt_record")) {
	    financesDS.where($"_corrupt_record".isNotNull).select($"_corrupt_record")
	              .write.mode(SaveMode.Overwrite).text("Output/corrupt_finances")
    }
    
    financesDS
              .map(tx => (s"${tx.account.firstName} ${tx.account.lastName}", tx.account.number))
			        .distinct
			        .toDF("FullName", "AccountNumber")
              .coalesce(5)
			        .write.mode(SaveMode.Overwrite).json("Output/finances-small-accounts")
			        
	  financesDS
              .select($"account.number".as("accountNumber").as[String], $"amount".as[Double], 
                      $"description".as[String], 
                      $"date".as[java.sql.Date](Encoders.DATE)).as[TransactionForAverage]
              .groupBy($"AccountNumber")
              .agg(avg($"Amount").as("average_transaction"), sum($"Amount").as("total_transactions"),
                   count($"Amount").as("number_of_transactions"), max($"Amount").as("max_transaction"),
                   min($"Amount").as("min_transaction"), stddev($"Amount").as("standard_deviation_amount"),
                   collect_set($"Description").as("unique_transaction_descriptions")) 
              .withColumnRenamed("accountNumber", "account_number")
              .coalesce(5)
              .write
              .mode(SaveMode.Overwrite)
              .cassandraFormat("account_aggregates", "finances")
              .save
  }
  
  implicit class DatasetHelper[T](ds: Dataset[T]) {
    import scala.util.Try //org.apache.spark.sql.AnalysisException
    def hasColumn(columnName: String) = Try(ds(columnName)).isSuccess
  }
}