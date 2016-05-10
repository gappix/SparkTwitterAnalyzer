package it.reti.spark.sentiment

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]) {
    val app = args(0) match {
      case "batch" => {
        val fileName = "/user/maria_dev/Tutorials/OpFelicita/LOMBARDIA.20160405-125641.json"
        new TweetBatchApp(fileName)
      }
      case  "streaming" => {
        println("\nSelect country to spy: \n \t\t 1: Australia \n \t\t 2: London \n \t\t 3: Busto Arsizio \n \t\t 4: USA")
        val location = Console.readLine()
        new TweetStreamingApp(location)
      }
      case default => {
        println("Parametro errato, passare batch o streaming")
        null
      }
    }
    
    val sc = new SparkContext(new SparkConf().setAppName("OPFelicitaS").setMaster("local[*]"))
    val sqlContext = new SQLContext(sc)
    val sqlContextHIVE = new HiveContext(sc)
    if (app != null) app.Run(sc, sqlContext, sqlContextHIVE)
    
  }
}