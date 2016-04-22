package it.reti.spark.twitter
import org.apache.spark.{SparkConf, SparkContext, rdd}
import scala.util.parsing.json._

object Program {
  val conf = new SparkConf()
      .setAppName("my-app")
      .setMaster("local")
  val sc = new SparkContext(conf)
  
  def readFile(fileName: String) : rdd.RDD[String] = {
    if (fileName.startsWith("hdfs://")) {
      throw new Exception("Must specify an hdfs location.");
    }
    
    println("Opening file " + fileName + "...")
    return sc.textFile(fileName);
  }
  
  def parseJson(jsonText : String) : Option[Any] = {
    return JSON.parseFull(jsonText)
  }
  
  def main(args: Array[String]) : Unit = {
    if (args.length <= 1) {
      println("Missing parameter, pass HDFS filename to parse.")
      System.exit(1)
    }
    
    val fileName = args(1);
    println("Using file " + fileName + " to read data...")
    
    val inputFile = sc.textFile(fileName);
    
    println("Transforming each file line to a JSON object...")
    inputFile.foreach { line =>
      var jsonObj = parseJson(line)
    }
  }
}