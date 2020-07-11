package main

import org.apache.spark.sql.{Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.cassandra._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

case class Account(number: String, firstName: String, lastName: String)
case class Transaction(id: Long, account: Account, date: java.sql.Date, amount: Double, 
                       description: String)
case class TransactionForAverage(accountNumber: String, amount: Double, description: String, 
                                 date: java.sql.Date)
case class SimpleTransaction(id: Long, account_number: String, amount: Double, 
                             date: java.sql.Date, description: String)
case class UnparsableTransaction(id: Option[Long], originalMessage: String, exception: Throwable)
case class AggregateData(totalSpending: Double, numTx: Int, windowSpendingAvg: Double) {
  val averageTx = if(numTx > 0) totalSpending / numTx else 0
}
case class EvaluatedSimpleTransaction(tx: SimpleTransaction, isPossibleFraud: Boolean)

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
              
    val checkpointDir = "file:///checkpoint"	
    val streamingContext = StreamingContext.getOrCreate(checkpointDir, () => {
	    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
		  ssc.checkpoint(checkpointDir)
		  val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "transactions-group", Map("transactions"->1))
	    kafkaStream.map(keyVal => tryConversionToSimpleTransaction(keyVal._2))
	               .flatMap(_.right.toOption)
				         .map(simpleTx => (simpleTx.account_number, simpleTx))
				         .window(Seconds(10), Seconds(10))
				         .updateStateByKey[AggregateData]((newTxs: Seq[SimpleTransaction], 
				                                          oldAggDataOption: Option[AggregateData]) => 
		  {
			  val calculatedAggTuple = newTxs.foldLeft((0.0,0))((currAgg, tx) => (currAgg._1 + tx.amount, currAgg._2 + 1))
			  val calculatedAgg = AggregateData(calculatedAggTuple._1, calculatedAggTuple._2, 
			                             if(calculatedAggTuple._2 > 0) calculatedAggTuple._1/calculatedAggTuple._2 else 0)
			  Option(oldAggDataOption match { 
				  case Some(aggData) => AggregateData(aggData.totalSpending + calculatedAgg.totalSpending, 
				                                      aggData.numTx + calculatedAgg.numTx, calculatedAgg.windowSpendingAvg)
					case None => calculatedAgg
				})
		  })
                 .transform(dStreamRDD => 
      {
			  val sc = dStreamRDD.sparkContext
			  val historicAggsOption = sc.getPersistentRDDs
					                         .values.filter(_.name == "storedHistoric").headOption
			  val historicAggs = historicAggsOption match {
				  case Some(persistedHistoricAggs) => 
				    persistedHistoricAggs.asInstanceOf[org.apache.spark.rdd.RDD[(String, Double)]]
					case None => {
					  import com.datastax.spark.connector._  
					  val retrievedHistoricAggs = sc.cassandraTable("finances", "account_aggregates")
					                                .select("account_number", "average_transaction")
						                              .map(cassandraRow => (cassandraRow.get[String]("account_number"), 
						                                                    cassandraRow.get[Double]("average_transaction")))
						retrievedHistoricAggs.setName("storedHistoric")
						retrievedHistoricAggs.cache
					}
				}
				dStreamRDD.join(historicAggs)
			})
				         .filter{ case (acctNum, (aggData, historicAvg)) => 
				           aggData.averageTx - historicAvg > 2000 || aggData.windowSpendingAvg - historicAvg > 2000 
				         }
				         .print
		  ssc
	  })
       
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