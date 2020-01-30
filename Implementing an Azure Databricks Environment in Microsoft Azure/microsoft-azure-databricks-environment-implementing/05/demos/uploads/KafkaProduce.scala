// Databricks notebook source
import java.util.Properties
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

// Configuration for Kafka brokers
val kafkaBrokers = "10.0.0.x:9092,10.0.0.x:9092,10.0.0.x:9092"

val topicName = "tweets"

val props = new Properties()
props.put("bootstrap.servers", kafkaBrokers)
props.put("acks", "all")
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

val producer = new KafkaProducer[String, String](props)

def sendEvent(message: String) = {
  val key = java.util.UUID.randomUUID().toString()
  producer.send(new ProducerRecord[String, String](topicName, key, message))
  System.out.println("Sent event with key: '" + key + "' and message: '" + message + "'\n")
}

// Twitter configuration!
// Replace values below with yours

val twitterConsumerKey=""
val twitterConsumerSecret=""
val twitterOauthAccessToken=""
val twitterOauthTokenSecret=""

import java.util._
import scala.collection.JavaConverters._
import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

val cb = new ConfigurationBuilder()
  cb.setDebugEnabled(true)
  .setOAuthConsumerKey(twitterConsumerKey)
  .setOAuthConsumerSecret(twitterConsumerSecret)
  .setOAuthAccessToken(twitterOauthAccessToken)
  .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

val twitterFactory = new TwitterFactory(cb.build())
val twitter = twitterFactory.getInstance()

// Getting tweets with certain keywords and sending them to Kafka in realtime!

val query = new Query(" #Azure ")
query.setCount(100)
query.lang("en")
var finished = false
while (!finished) {
  val result = twitter.search(query)
  val statuses = result.getTweets()
  var lowestStatusId = Long.MaxValue
  for (status <- statuses.asScala) {
    if(!status.isRetweet()){
      sendEvent(status.getText())
    }
    lowestStatusId = Math.min(status.getId(), lowestStatusId)
    Thread.sleep(500)
  }
  query.setMaxId(lowestStatusId - 1)
}