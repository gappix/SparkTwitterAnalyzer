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



//////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * This class is an extension of TweetApp one.
 * It implements a specific Run method for streaming data retrieving from a twitter stream spout.
 * It then uses upper-class methods for data elaboration and result storing.  
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TweetStreamingApp(locationToObserve : String) extends TweetApp(locationToObserve) {
  
  

  
  
  //.........................................................................................................
  /**
   * Run method OVERRIDED in order to fulfill streaming app processing needings
   * It takes spout source data and packs everything into a single DataFrame every 25 seconds.
   * Each DataFrame structure is then passed to upper-class "elaborate" method in order to retrieve sentiment evaluation.
   * Results are eventually stored into HIVE tables by invoking upper-class "storeDataFrameToHIVE" method.
   */
  override def Run() {  
    
    
    
    //import context needed
    val sc = ContextHandler.getSparkContext
    val ssc = new StreamingContext(sc, Seconds(25))
    val sqlContextHIVE = ContextHandler.getSqlContextHIVE  
    
  
    
  
  
     /*---------------------------------------------------
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

    
    
    
    /*-----------------------------------------------
     * Stream input and transformations
     *-----------------------------------------------*/
    
    //input tweets 
    /*<<< INFO >>>*/ LogHandler.log.info("Opening Twitter stream...")
    val streamTweets = TwitterUtils.createStream(ssc, None, new Array[String](0) ,StorageLevel.MEMORY_AND_DISK)    
    /*<<< INFO >>>*/ LogHandler.log.info("Stream opened!")

    
    
    
    /*<<< DEBUG >>>*/ LogHandler.log.debug("received " + streamTweets.count().toString() + " messages")//<<< DEBUG
    
    
    
    //filtering by language and coordinates
    val englishTweets = streamTweets
                              .filter { status => status.getLang=="en"}
                              .filter { status =>  myLocation.checkLocation(status) }
       
                              
                              
     
    /*----------------------------------------------
     * Transformations for each RDD
     *----------------------------------------------*/     
    englishTweets.foreachRDD { rdd    => 
      
      
      /*<<< INFO >>>*/ LogHandler.log.info( rdd.toString() + " Starting Tweet block processing...")
      
      
      /*.........................................
       * UDF definition
       *.........................................*/
      
      //sanitization with lowercase and standard-word charachters only
      val sanitizeTweet = udf ((word: String) => {
                                      val regularExpression = "\\w+(\'\\w+)?".r 
                                      val sanitizedWord = regularExpression.findFirstIn(word.toLowerCase)
                                      val emptyWord = ""
                                      sanitizedWord match {
                                        case None            => emptyWord
                                         case Some(something) => something
                                      }
                                })
                                        
      // confidency rate evaluation
      val confidencyValue = udf( (matched_words: Double, tweet_words: Double) => matched_words/tweet_words)
          
      
      
     /*..........................................
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
     
      
      
      //Elaborate method invoked at every RDD
      val elaboratedTweets = Elaborate(readyTweetsDF)
      
      
      /*<<< INFO >>>*/ LogHandler.log.info(rdd.toString() + " Elaboration completed!") 
      /*<<< INFO >>>*/ LogHandler.log.info(rdd.toString() + " Sending DataFrames to HIVE Storage...")
      
      
      //store DataFrames to HIVE tables
      storeDataFrameToHIVE( elaboratedTweets.allTweets, elaboratedTweets.sentimentTweets) 
       
      
      
      /*<<< INFO >>>*/ LogHandler.log.info(rdd.toString() +  " Processing completed!")
      
      
      
        }//end foreachRDD
 
     /*------------------------------------------
      * Streaming start, await and stop
      *------------------------------------------*/
      
      //graceful stop 
      sys.ShutdownHookThread {
      /*<<< INFO >>>*/ LogHandler.log.info("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      /*<<< INFO >>>*/  LogHandler.log.info("Application gracefully stopped")
      }
      
      ssc.start()
      ssc.awaitTermination()
    
  }//end Run overrided method //
  
  
  
  
  //...........................................................................................................
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
  
  
  
  
  
  //...........................................................................................................
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

