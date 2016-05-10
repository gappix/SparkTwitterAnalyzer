package it.reti.spark.sentiment

import scala.reflect.runtime.universe

import org.apache.spark.SparkContext

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
    

/**==================================================================================================================
 *            Class Hedonometer  
 *===================================================================================================================*/
    
class Hedonometer(sc: SparkContext) extends Serializable{
  
   /*-------------------------------------------------------------------------------------------
    * SPARK HIVE environment configuration
    *-------------------------------------------------------------------------------------------*/
    private val sqlContextHIVE = new org.apache.spark.sql.hive.HiveContext(sc)
    
    //import methods for DataFrame/RDD conversion
    import sqlContextHIVE.implicits._
    
    /*---------------------------------------------------------------------------------------------
     * Hedonometer loading and dataframe transformation
     *---------------------------------------------------------------------------------------------*/
    

    //loading from txt file
    private val inputHEDONOMETER = sc.textFile("/user/maria_dev/Tutorials/OpFelicitaS/hedonometerNOHEADER.txt")
    
    //splitting fields
    private val hedonometerDF = inputHEDONOMETER
         //splitto quando trovo degli spazi
         .map(_.split("\t")) 
         //associo la case Class HEDO con la relativa struttura
         .map( p => Hedo( p(0), p(1).toInt, p(2).toFloat, p(3).toFloat, p(4), p(5), p(6))) 
         //trasformo in DataFrame l'RDD risultante
         .toDF
              
    /**     
     *  Method to return the entire Hedonometer DataFrame   
     */
    def getHedonometer  = hedonometerDF
    
}

