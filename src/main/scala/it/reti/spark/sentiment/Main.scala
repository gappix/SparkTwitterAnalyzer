package it.reti.spark.sentiment

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf




//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 *  Main object for the SPARK Twitter Analyzer app            
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////  
object Main {
  

  def main(args: Array[String]) {
    
    
    /*INPUT parameter check:
     * according to input parameter string a different TweetApp extension class is instantiated.
     */
     val app = args(0) match {
      
       //BATCH case
        case "batch" => {
          val fileName = "/user/maria_dev/Tutorials/SPARKTwitterAnalyzer/RawTweets.json"
          new TweetBatchApp(fileName)
        }
        //STREAMING case
        case "streaming" => {
          println("\nSelect country to spy: \n \t\t 1: Australia \n \t\t 2: London \n \t\t 3: Busto Arsizio \n \t\t 4: USA")
          val location = Console.readLine()
          new TweetStreamingApp(location)
        }
        //Otherwise
        case default => {
          println("Wrong input parameter: write 'batch' or 'streaming'")
          null
        }
    }
    
    
    
 
    if (app != null ) {
      
      //SPARK Contexts creation
      val context = ContextHandler.setAllContexts
      
      //run execution
      if(context.equals("ok")) app.Run()
    }
    
    
  }//end main method //
  
  
  
}//end main object //