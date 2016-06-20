package it.reti.spark.sentiment

//imports needed
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Logging


//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * This object instantiates all SPARK Contexts once, and then retrieves them with appropriate methods
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
object ContextHandler extends Logging {
  
  
  
  /*<<<INFO>>>*/ logInfo("creating SPARK Contexts...")
  
  //SPARK contexts creation
  private val sc = new SparkContext(new SparkConf().setAppName("SPARK Twitter Analyzer").setMaster("local[*]"))
  private val sqlContext = new SQLContext(sc)
  private val sqlContextHIVE = new HiveContext(sc)
  
  private val status = "ok"
   
  /*<<<INFO>>>*/ logInfo("Contexts created!")

  
  //....................................................................................................................
  /**
   * method to instantiate object and check if successful
   * @return string status
   */
  def setAllContexts = status
  
  
  
  //....................................................................................................................
  /**
   * method that
   * @return active SparkContext
   */
  def getSparkContext = sc
  
  
  
  //....................................................................................................................
  /**
   * method that
   * @return active sqlContext
   */
  def getSqlContext = sqlContext
  
  
  
  //....................................................................................................................
  /**
   * method that
   * @return active Hive context
   */
  def getSqlContextHIVE = sqlContextHIVE
  
  
  
  
}//end ContextHandler object //