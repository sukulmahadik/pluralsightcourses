package main

import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.cassandra._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector._

case class Account(number: String, firstName: String, lastName: String)
case class Transaction(id: Long, account: Account, date: java.sql.Date, amount: Double, 
                       description: String)
case class TransactionForAverage(accountNumber: String, amount: Double, description: String, 
                                 date: java.sql.Date)
case class SimpleTransaction(id: Long, account_number: String, amount: Double, 
                             date: java.sql.Date, description: String)
case class UnparsableTransaction(id: Option[Long], originalMessage: String, exception: Throwable)

object Detector {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
                            .master("local[3]")
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
              
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(1))
    val kafkaStream = KafkaUtils.createStream(streamingContext, "localhost:2181", 
                                                                "transactions-group", Map("transactions"->1))
	
	  kafkaStream.map(keyVal => tryConversionToSimpleTransaction(keyVal._2))
	             .flatMap(_.right.toOption)
			         .foreachRDD(txRDD => txRDD.saveToCassandra("finances", "transactions"))
                                                                
	  streamingContext.start
	  streamingContext.awaitTermination
  }
  
  import scala.util.{Either, Right, Left}
  def tryConversionToSimpleTransaction(logLine: String): Either[UnparsableTransaction, SimpleTransaction] = {
	  import java.text.SimpleDateFormat
	  import scala.util.control.NonFatal
	  logLine.split(',') match {
      case Array(id, date, acctNum, amt, desc) => 
		    var parsedId: Option[Long] = None
		    try {
		      parsedId = Some(id.toLong)
          Right(SimpleTransaction(parsedId.get, acctNum, amt.toDouble, 
              new java.sql.Date((new SimpleDateFormat("MM/dd/yyyy")).parse(date).getTime), desc))
		    } catch {
		      case NonFatal(exception) => Left(UnparsableTransaction(parsedId, logLine, exception))
		    }
      case _ => Left(UnparsableTransaction(None, logLine, 
                     new Exception("Log split on comma does not result in a 5 element array.")))
	  }
  }
  
  implicit class DatasetHelper[T](ds: Dataset[T]) {
    import scala.util.Try //org.apache.spark.sql.AnalysisException
    def hasColumn(columnName: String) = Try(ds(columnName)).isSuccess
  }
}