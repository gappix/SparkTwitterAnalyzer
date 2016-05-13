package it.reti.spark.sentiment
import org.apache.log4j.Logger 



//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * This object gives a transient reference to Logger methods to permit cluster distributed logging operations
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
object LogHandler extends Serializable {
  
  
  
  //defining a lazy log variable
  @transient private lazy val myLog = Logger.getLogger(getClass.getName)
  
  
  
  //.................................................................................................................
  /**
   * method to 
   * @return myLog lazy variable
   */
  def log = myLog
 
  
  
  
}// end LogHandler class //