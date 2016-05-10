package it.reti.spark.samples

import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

object ServerWordCount {
  def main(args: Array[String]) = {
    if (args.length < 1) {
      System.err.println("Usage: ServerWordCount <port>")
      System.exit(1)
    }
    val Array(port) = args
    
    val conf = new SparkConf().setAppName("ServerWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    // Create a DStream that will open a port connection on the specified port
    val lines = ssc.receiverStream(new SocketReceiver(port.toInt))
    
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
