package it.reti.spark.sentiment

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * This object instantiates all SPARK Contexts once, and then retrieves them with appropriate methods.
 */
object ContextHandler {
  private val logger = Logger.getLogger(getClass.getName)
  
  logger.info("Creating SPARK Contexts...")
    
  // Spark contexts creation
  private val sc = new SparkContext(new SparkConf().setAppName("SPARK Twitter Analyzer").setMaster("local[*]"))
  private val sqlContext = new SQLContext(sc)
  private val sqlContextHIVE = new HiveContext(sc)
  
  private val status = "ok"
  
  logger.info("Contexts created!")
  
  /**
   * Method to instantiate object and check if successful.
   * @return string status
   */
  def setAllContexts = status
  
  /**
   * Method that returns the active Spark context.
   * @return the SparkContext
   */
  def getSparkContext = sc
  
  /**
   * Method that returns the active SQL context.
   * @return the sqlContext
   */
  def getSqlContext = sqlContext
  
  /**
   * Method that returns the active Hive context.
   * @return the active Hive context
   */
  def getSqlContextHIVE = sqlContextHIVE
   
}