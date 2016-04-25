package it.reti.spark.streaming

import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object NetworkWordCount {
  def main(args: Array[String]) = {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }
    val Array(hostname, port) = args
    
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    // Create a DStream that will connect to hostname:port, example localhost:9999
    val lines = ssc.socketTextStream(hostname, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    
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
