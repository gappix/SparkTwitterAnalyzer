package it.reti.spark.sentiment

import scala.reflect.runtime.universe
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator



//case class for Hedonometer structure (needed for DataFrame creation)
case class Hedo(
    dictionary: String,
    rank: Int,
    sentiment_value: Float,
    other1: Float,
    other2: String,
    other3: String,
    other4: String
    )
    
    
    
/*°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°*/
/**
 * This class loads the hedonometer dictionary from a HDFS text file and structures it as DataFrame
 * It can be accessed by other classes by a getHedonometer invocation   
 */    
object Hedonometer extends Serializable with Logging{
  
  
  
    //getting Contexts
    val sc = ContextHandler.getSparkContext
    val sqlContextHIVE = ContextHandler.getSqlContextHIVE
    //import methods for DataFrame/RDD conversion
    import sqlContextHIVE.implicits._
    
    
    
    //load textfile RDD
    private val inputHEDONOMETER = sc.textFile("/user/maria_dev/Tutorials/SPARKTwitterAnalyzer/HedonometerNOHEADER.txt")
    
    
    
    //DataFrame creation
    private val hedonometerDF = inputHEDONOMETER 
                               .map(_.split("\t")) //split on tab spaces 
                               .map( //RDD association with Hedo case class
                                     p => Hedo( p(0), p(1).toInt, p(2).toFloat, p(3).toFloat, p(4), p(5), p(6))
                                     ) 
                               .toDF //DataFrame creation
     
    
    //BROADCAST the data loaded to all cluster nodes
    private val broadCastedHedonometer = sc.broadcast(hedonometerDF)
     
    

    //.............................................................................................................     
    /**     
     *  Method to 
     *  @return the entire hedonometer dictionary DataFrame from local broadcasted variable
     */
    def getHedonometer  = broadCastedHedonometer.value
      


    
    
}// end  Hedonometer class //

