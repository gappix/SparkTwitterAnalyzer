package it.reti.spark.sentiment

import scala.reflect.runtime.universe

import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils

/**
 * This class is an extension of TweetApp one.
 * It implements a specific Run method for streaming data retrieving from a twitter stream spout.
 * It then uses upper-class methods for data elaboration and result storing.  
 */
class TweetStreamingApp(locationToObserve : String) extends TweetApp(locationToObserve) {
  private val logger = Logger.getLogger(getClass.getName)
  
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
 
    logger.info("Opening Twitter stream...")
    val streamTweets = TwitterUtils.createStream(ssc, None, new Array[String](0), StorageLevel.MEMORY_AND_DISK)    
    logger.info("Stream opened!")
    logger.debug("received " + streamTweets.count().toString() + " messages")//<<< DEBUG
    
    //filtering by language and coordinates
    val englishTweets = streamTweets
        .filter { status => status.getLang=="en" }
        .filter { status => myLocation.checkLocation(status) }

    englishTweets.foreachRDD { rdd => 
      logger.info(rdd.toString() + " Starting Tweet block processing...")
      
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
      val confidencyValue = udf ((matched_words: Double, tweet_words: Double) => matched_words/tweet_words)
          
      val schema = StructType(Array(
          StructField("tweet_id", LongType, true), 
          StructField("lang", StringType, true),
          StructField("user_id", LongType, true),
          StructField("user_name", StringType, true), 
          StructField("bb_latitude", DoubleType, true),    
          StructField("gl_latitude", DoubleType, true),
          StructField("bb_longitude", DoubleType, true),    
          StructField("gl_longitude", DoubleType, true),
          StructField("text", StringType, true) 
      ))       

      //ready tweet with correct info extraction
      val readyTweetsDF = sqlContextHIVE.createDataFrame(rdd.map(status => Row(
          status.getId,                         //tweet_id
          status.getLang,                       //lang
          status.getUser.getId,                 //user_id
          status.getUser.getName,               //user_name 
          getBoundingBoxCoordinates(status)._1, //bb_latitude
          getGeoLocationCoordinates(status)._1, //gl_latitude
          getBoundingBoxCoordinates(status)._2, //bb_longitude
          getGeoLocationCoordinates(status)._2, //gl_longitude
          status.getText                        //text
      )), schema)
     
      //Elaborate method invoked at every RDD
      val elaboratedTweets = Elaborate(readyTweetsDF)
      logger.info(rdd.toString() + " Elaboration completed!") 
      logger.info(rdd.toString() + " Sending DataFrames to HIVE Storage...")
      
      //store DataFrames to HIVE tables
      storeDataFrameToHIVE( elaboratedTweets.allTweets, elaboratedTweets.sentimentTweets) 
      logger.info(rdd.toString() +  " Processing completed!")
    }
 
    //graceful stop 
    sys.ShutdownHookThread {
      logger.warn("Gracefully stopping Spark Streaming Application.")
      ssc.stop(true, true)
      logger.info("Application gracefully stopped.")
    }
   
    ssc.start()
    ssc.awaitTermination()
  }
  
  /**
   * Method that returns latitude and longitude retrieved from the tweet.
   * @return (GeoLocation latitude, GeoLocation longitude) if present, (None None) otherwise
   */
  def getGeoLocationCoordinates(status : twitter4j.Status) : (Option[Double], Option[Double]) = {
    status.getGeoLocation match {
        case null => (None, None)
        case default => (Some(status.getGeoLocation.getLatitude),Some(status.getGeoLocation.getLongitude))
    }
  }

  /**
   * Method that returns bounding box coordinates if present.
   * @return (Place latitude, Place longitude) if present, (None None) otherwise
   */
  def getBoundingBoxCoordinates(status : twitter4j.Status) : (Double, Double) = {
    if (status != null && status.getPlace != null && status.getPlace.getBoundingBoxCoordinates != null) {
      return (status.getPlace.getBoundingBoxCoordinates.head.head.getLatitude, status.getPlace.getBoundingBoxCoordinates.head.head.getLongitude)
    }
    else {
      return (null.asInstanceOf[Double], null.asInstanceOf[Double])
    }
  }
}
