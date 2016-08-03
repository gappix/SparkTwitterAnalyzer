package it.reti.spark.sentiment
import org.apache.log4j.Logger 
import org.apache.log4j.Level



//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * This object gives a transient reference to Logger methods to permit cluster distributed logging operations
 */
object LogHandler extends Serializable {
  
  
 /*--------------------------------------------------
  * SETTING LOG LEVELS
  *---------------------------------------------------*/
  Logger.getRootLogger.setLevel(Level.WARN)
  
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("it.reti.spark.sentiment").setLevel(Level.INFO)
  Logger.getLogger("it.reti.spark.sentiment.TweetApp").setLevel(Level.DEBUG)
  
  
  
  
  
  
  //defining a lazy log variable
  @transient private lazy val myLog = Logger.getLogger(getClass.getName)
  

  
  //.................................................................................................................
  /**
   * method to 
   * @return myLog lazy variable
   */
  def log = myLog
 
  
  
  
}// end LogHandler class //