package it.reti.spark.samples

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) = {
    if (args.length < 3) {
      System.err.println("Usage: <broker-list> <zk-list> <topic>")
      System.exit(1)
    }
    val Array(broker, zk, topic) = args
    
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    val kafkaConf = Map("metadata.broker.list" -> broker,
                        "zookeeper.connect" -> zk,
                        "group.id" -> "kafka-spark-streaming-example",
                        "zookeeper.connection.timeout.ms" -> "1000")

    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder] (
        ssc, kafkaConf, Map(topic -> 1),
        StorageLevel.MEMORY_ONLY_SER
      ).map(_._2)
    
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    
    // Start the computation
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}
