package it.reti.spark.sentiment

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 *  Main object for the SPARK Twitter Analyzer app            
 */
object Main {
  def main(args: Array[String]) {
    // Checking input parameters.
    // According to input parameter string a different TweetApp extension class is instantiated.
    val app = args(0) match {
      case "batch" => {
        val fileName = "/user/maria_dev/Tutorials/SPARKTwitterAnalyzer/RawTweets.json"
        new TweetBatchApp(fileName)
      }
      case "streaming" => {
        println("\nSelect country to spy: \n \t\t 1: Australia \n \t\t 2: London \n \t\t 3: Busto Arsizio \n \t\t 4: USA")
        val location = Console.readLine()
        new TweetStreamingApp(location)
      }
      case default => {
        println("Wrong input parameter: write 'batch' or 'streaming'")
        null
      }
    }
    
    if (app != null) {
      val context = ContextHandler.setAllContexts //SPARK Contexts creation
      if (context.equals("ok")) app.Run() //run execution
    }
  }
}