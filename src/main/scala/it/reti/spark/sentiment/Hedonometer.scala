package it.reti.spark.sentiment

import scala.reflect.runtime.universe
import org.apache.spark.SparkContext

/**
 * case class for Hedonometer structure (needed for DataFrame creation)
 */
case class Hedo(
  dictionary: String,
  rank: Int,
  sentiment_value: Float,
  other1: Float,
  other2: String,
  other3: String,
  other4: String
)
    
/**
 * This class loads the hedonometer dictionary from a HDFS text file and structures it as DataFrame.
 * It can be accessed by other classes by a getHedonometer method invocation.
 */
@SerialVersionUID(9712126469L)
class Hedonometer() extends Serializable {
  val fileName = "/user/maria_dev/Tutorials/SPARKTwitterAnalyzer/HedonometerNOHEADER.txt"
  
  // getting Contexts
  val sc = ContextHandler.getSparkContext
  val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  // import methods for DataFrame/RDD conversion
  import sqlContextHIVE.implicits._
    
  //load textfile RDD
  private val inputHEDONOMETER = sc.textFile(fileName)
    
  //DataFrame creation
  private val hedonometerDF = inputHEDONOMETER 
    .map(_.split("\t")) //split on tab spaces 
    .map(p => Hedo( p(0), p(1).toInt, p(2).toFloat, p(3).toFloat, p(4), p(5), p(6))) //RDD association with Hedo case class 
    .toDF //DataFrame creation
          
  /**     
   *  Method that returns the entire hedonometer dictionary DataFrame. 
   *  @return the entire hedonometer dictionary DataFrame  
   */
  def getHedonometer  = hedonometerDF
    
}

