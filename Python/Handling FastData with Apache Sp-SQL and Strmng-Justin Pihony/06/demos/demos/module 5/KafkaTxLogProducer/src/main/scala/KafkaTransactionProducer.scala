package main

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, Producer, ProducerRecord}
import org.apache.kafka.clients.producer

object KafkaTransactionProducer{
  /*
   * Usage 
   * scala -classpath KafkaTransactionLogProducer-0.1.jar main.KafkaTransactionProducer SERVERS TOPIC RANDOMIZE
   * SERVERS is a comma delimited list of servers in the format of [ServerAddress]:[Port] ~ Default = localhost:9092
   * TOPICS is the Kafka topic you want to produce messages to ~ Default = transactions
   * RANDOMIZE is a yes/no flag to turn on/off randomization of transaction output ~ Default = no
   */
  def main(args: Array[String]) {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.lift(0).getOrElse("localhost:9092"))
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
	
	val accountNumbers = List("123-ABC-789", "456-DEF-456", "333-XYZ-999", "987-CBA-321")
	val descriptions = List("Drug Store", "Grocery Store", "Electronics", "Park", "Gas", "Books", "Movies", "Misc")
	val transactionAmounts = List(10.34, 94.65, 2.49, 306.21, 1073.12, 20.00, 7.92, 4322.33)
 
    val producer = new KafkaProducer[Nothing, String](props)
	
	System.out.print(">>>Press [ENTER] to shut the producer down")
	val topic = args.lift(1).getOrElse("transactions")
	val randomize = args.lift(2).map(_.toLowerCase).getOrElse("no") == "yes"
	var currentStep = 0
    while(System.in.available == 0 || (!randomize && currentStep <= 100)){
	  val delayUntilNextSend = if(randomize) scala.util.Random.nextInt(5000) else ((currentStep + 1) * 50) //Up to 5 seconds
	  Thread.sleep(delayUntilNextSend)
	  val accountNumber = if(randomize) accountNumbers(scala.util.Random.nextInt(accountNumbers.size)) else accountNumbers(currentStep % accountNumbers.size)
	  val description = if(randomize) descriptions(scala.util.Random.nextInt(descriptions.size)) else descriptions(currentStep % descriptions.size) 
	  val currentDate = (new java.text.SimpleDateFormat("MM/dd/yyyy")).format(new java.util.Date())
	  val txAmount = if(randomize) math.floor((scala.util.Random.nextInt(5000) + scala.util.Random.nextDouble) * 100) / 100 else transactionAmounts(currentStep % transactionAmounts.size) 
	  val transactionLogLine = s"$currentStep,$currentDate,$accountNumber,$txAmount,$description"
	  producer.send(new ProducerRecord(topic, transactionLogLine))
	  println("Sent -> " + transactionLogLine)
	  currentStep = currentStep + 1
	}
    
	producer.close()
  }
}

