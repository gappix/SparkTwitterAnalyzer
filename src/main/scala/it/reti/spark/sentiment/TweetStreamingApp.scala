package it.reti.spark.sentiment

import scala.reflect.runtime.universe

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.spark.Logging



/*°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°*/
/**
 * This class is an extension of TweetApp one.
 * It implements a specific Run method for streaming data retrieving from a twitter stream spout.
 * It then uses upper-class methods for data elaboration and result storing.  
 * 
 * @param locationToObserve: String containing an integer number according to Location class options
 */
class TweetStreamingApp(locationToObserve : String) extends TweetApp("streaming") with Logging {
  
  

  
  
  
  /*.................................................................................................................*/
  /**
   * Run method OVERRIDED in order to fulfill streaming app processing needings
   * It takes spout source data and packs everything into a single DataFrame every 25 seconds.
   * Each DataFrame structure is then passed to upper-class "elaborate" method in order to retrieve sentiment evaluation.
   * Results are eventually stored into HIVE tables by invoking upper-class "storeDataFrameToHIVE" method.
   */
  override def Run() {  
    
    
    //interval settings
    val intervalSeconds = 25 /* <---- set this to best interval for your operation */ 
    
    //import context needed
    val sc = ContextHandler.getSparkContext
    val ssc = new StreamingContext(sc, Seconds(intervalSeconds))
    val sqlContextHIVE = ContextHandler.getSqlContextHIVE  

  
     /*----------------------------------------------------
     * Twitter Auth Settings
     * >>>>>>> maybe in a .properties file?
     *----------------------------------------------------*/
    
      //twitter auth magic configuration
      val consumerKey = "hARrkNBpwsh8lLldQt7fTe4iM"
      val consumerSecret = "p0BRXCYEePUrJXPHQBxdIkP14idAYaSi934VJU2Hm2LBCUuqg0"
      val accessToken = "72019464-oUaReZ3i91fcVKg3Y7mBOzxlNrNXMpa5sxOcIld3R"
      val accessTokenSecret = "338I4ldbMc3CDpGYrpx5BuDfYbcAAZbJRDW86i9EY6Nwf"
      
      //setting system properties so that Twitter4j library can use general OAuth credentials
      System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
      System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
      System.setProperty("twitter4j.http.proxyHost", "10.1.8.13");
      System.setProperty("twitter4j.http.proxyPort", "8080");
    

    
    val myLocation = new Location(augmentString(locationToObserve).toInt)
    
    //optionally set word filters on input data
    val filters = new Array[String](0)
    
    
    
    /*------------------------------------------------
     * Stream input and transformations
     *-----------------------------------------------*/
    
    //input tweets 
    /*<<< INFO >>>*/ logInfo("Opening Twitter stream...")
    val streamTweets = TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_ONLY_SER)    
    /*<<< INFO >>>*/ logInfo("Stream opened!")

    
    
    

    
    
    //filtering by language and coordinates
    val englishTweets = streamTweets 
                              .filter { status => status.getLang=="en"}
                              .filter { status =>  myLocation.checkLocation(status) }
      
                            
   
    
   /*----------------------------------------------------
 	  * Transformations for each RDD
   	*---------------------------------------------------*/     
    englishTweets.foreachRDD { rdd    => 

    

    /*<<< INFO >>>*/ logInfo(rdd.toString() +  " started!")
    
    
    
    
   /*...........................................
     * DataFrame creation
     *.........................................*/     
    
    val schema = StructType( 
                              Array( StructField("tweet_id", LongType, true), 
                                     StructField("lang", StringType, true),
                                     StructField("user_id", LongType, true),
                                     StructField("user_name", StringType, true), 
                                     StructField("bb_latitude", DoubleType, true),    
                                     StructField("gl_latitude", DoubleType, true),
                                     StructField("bb_longitude", DoubleType, true),    
                                     StructField("gl_longitude", DoubleType, true),
                                     StructField("text", StringType, true) 
                                    )//end of Array definition
                              )//end of StructType       
          
                              
                              
                              
    //ready tweet with correct info extraction
    val readyTweetsDF = sqlContextHIVE.createDataFrame(  rdd.map(   
                                        status =>   Row(
                                                        status.getId,                         //tweet_id
                                                        status.getLang,                       //lang
                                                        status.getUser.getId,                 //user_id
                                                        status.getUser.getName,               //user_name 
                                                        getBoundingBoxCoordinates(status)._1, //bb_latitude
                                                        getGeoLocationCoordinates(status)._1, //gl_latitude
                                                        getBoundingBoxCoordinates(status)._2, //bb_longitude
                                                        getGeoLocationCoordinates(status)._2, //gl_longitude
                                                        status.getText                        //text
                                                        
                                                      )
                                                                    ), schema)//end of createDataFrame
      
                                                        
                                                                    
      /*<< INFO >>*/logInfo("Received " + readyTweetsDF.persist().count.toString() + " tweets") 
      readyTweetsDF.show()
      
      
     //checking if there is any message to process 
     if(readyTweetsDF.count() > 0){
       

       
       
          //Elaborate method invoked at every RDD
          val elaboratedTweets = Elaborate(readyTweetsDF)
           
          
          
          
  
          //store DataFrames to HIVE tables
          storeDataFrameToHIVE( elaboratedTweets.allTweets, elaboratedTweets.sentimentTweets) 
          
          //free memory
          readyTweetsDF.unpersist()
          /*<<< INFO >>>*/ logInfo(rdd.toString() +  " Processing completed!")
       
          
      }//end if
    
    
    
    
     /*<<< INFO >>>*/ logInfo("\n\n ========================================== END ROUND ============================================>>>\n\n\n")
    

       
    }//end foreachRDD
 
    
  
    
    
    
    
   /*------------------------------------------------
    * Streaming start, await and stop
    *-----------------------------------------------*/

    ssc.start()
    ssc.awaitTermination()
    
    
  }//end Run overrided method //
  
  
  
  
  /*.................................................................................................................*/
  /**
   * Method that
   * @return (GeoLocation latitude, GeoLocation longitude) if present, (None None) otherwise
   */
  def getGeoLocationCoordinates( status : twitter4j.Status) : (Option[Double], Option[Double]) = {
     
    
    status.getGeoLocation match{
      
      case null => (None, None)
      case default => (Some(status.getGeoLocation.getLatitude),Some(status.getGeoLocation.getLongitude))
   
    }
    

  }// end getGeoLocationCoordinates method //
  
  
  
  
  
  /*.................................................................................................................*/
  /**
   * Method that
   * @return (Place latitude, Place longitude) if present, (None None) otherwise
   */
  def getBoundingBoxCoordinates(status : twitter4j.Status) : (Double, Double) = {
    
    
    if (status != null && status.getPlace != null && status.getPlace.getBoundingBoxCoordinates != null) {
      return (status.getPlace.getBoundingBoxCoordinates.head.head.getLatitude, status.getPlace.getBoundingBoxCoordinates.head.head.getLongitude)
    }
    else {
      return (null.asInstanceOf[Double], null.asInstanceOf[Double])
    }
    
    
  }// end getBoundingBoxCoordinates //
  
  
  
   
}//end TweetStreamingApp class //

