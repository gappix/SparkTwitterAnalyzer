package it.reti.spark.sentiment

import scala.reflect.runtime.universe

import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
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
import javax.annotation.Nullable
import org.apache.spark.sql.Column
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

/** =========================================================================================================
* main object (singleton) for SPARK STREAMING tweet processing
* 
 * 
 ===========================================================================================================*/

class TweetStreamingApp(runParam : String) extends TweetApp(runParam) {
  
  
  override def Run() {
  val sc = new SparkContext(new SparkConf().setAppName("OPFelicitaS").setMaster("local[*]"))
  val sqlContext = new SQLContext(sc)
  val sqlContextHIVE = new HiveContext(sc)
  val ssc = new StreamingContext(sc, Seconds(30))
  
  //import methods for DataFrame/RDD conversion
  import sqlContext.implicits._
    
     /*-------------------------------------------------------------------------------------------
     * Twitter Auth Settings
     *-------------------------------------------------------------------------------------------*/
    
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
    
    /*-------------------------------------------------------------------------------------------
     * Starting
     *-------------------------------------------------------------------------------------------*/
    
    val myLocation = new Location(augmentString(runParam).toInt)

    
    /*-------------------------------------------------------------------------------------------
     * Stream input and transformations
     *-------------------------------------------------------------------------------------------*/
    
    //input tweets 
    val streamTweets = TwitterUtils.createStream(ssc, None)    

    //filtering by language and coordinates
     val englishTweets = streamTweets
           .filter { status => status.getLang=="en"}
           .filter { status =>  myLocation.checkLocation(status) }
     
     /*-------------------------------------------------------------------------------------------
      * Transformations for each RDD
      *-------------------------------------------------------------------------------------------*/     
      englishTweets.foreachRDD { rdd =>  
   
               /*.........................................................................................
               * SQL envirnoment configuration
               *.........................................................................................*/
                val sqlContextHIVE = new HiveContext(rdd.sparkContext)
                import sqlContextHIVE.implicits._
            
                
              /*.........................................................................................
               * User Defined Functions declarations
               *.........................................................................................*/
              
                //sanitization with lowercase and standard-word charachters only
                val sanitizeTweet = udf (( word: String) => {
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
              
              /*.........................................................................................
               * RDD to DataFrame
               *.........................................................................................*/
              
                val schema = StructType( 
                                         Array( StructField( "tweet_id", LongType, true), 
                                                StructField( "lang", StringType, true),
                                                StructField( "user_id", LongType, true),
                                                StructField( "user_name", StringType, true), 
                                                StructField("bb_latitude", DoubleType, true),    
                                                StructField("gl_latitude", DoubleType, true),
                                                StructField("bb_longitude", DoubleType, true),    
                                                StructField("gl_longitude", DoubleType, true),
                                                StructField("text", StringType, true) 
                                                )//end of Array definition
                                                
                                         )//end of StructType       
                
                //ready tweet with correct info extraction
                val readyTweetsDF = sqlContextHIVE.createDataFrame(  
                  rdd.map(   status =>   Row(
                      status.getId,          //tweet_id
                      status.getLang,        //lang
                      status.getUser.getId,  //user_id
                      status.getUser.getName,//user_name 
                      getBoundingBoxCoordinates(status)._1, //bb_latitude
                      getGeoLocationCoordinates(status)._1, //gl_latitude
                      getBoundingBoxCoordinates(status)._2, //bb_longitude
                      getGeoLocationCoordinates(status)._2, //gl_longitude
                      status.getText        //text
                      )//end of Row
               ), schema);//end of createDataFrame
               
                val elaboratedTweets = Elaborate(readyTweetsDF)
                
                elaboratedTweets.allTweets.show()
                elaboratedTweets.sentimentTweets.show()
                
                elaboratedTweets.allTweets.registerTempTable("processed_tweets_TEMP")
                elaboratedTweets.sentimentTweets.registerTempTable("tweet_sentiment_TEMP")
                
                //querying HIVE to store tempTable contents
                sqlContextHIVE.sql("CREATE TABLE IF NOT EXISTS tweets_processed_3 (`tweet_id` bigint, `lang` string, `user_id` bigint, `user_name` string, `latitude` float, `longitude` float, `text` string) STORED AS ORC")
                sqlContextHIVE.sql("CREATE TABLE IF NOT EXISTS tweets_sentiment_3 (`tweet_id` bigint, `sentiment_value` float, `matched_words` int, `tweet_words` int, `confidency_value` float) STORED AS ORC")
                sqlContextHIVE.sql("INSERT INTO TABLE tweets_processed_3 SELECT * FROM processed_tweets_TEMP")
                sqlContextHIVE.sql("INSERT INTO TABLE tweets_sentiment_3 SELECT * FROM tweet_sentiment_TEMP")
    
     }//end foreachRDD
 
   /*-------------------------------------------------------------------------------------------
    * Streaming start and await
    *-------------------------------------------------------------------------------------------*/ 
    ssc.start()
    ssc.awaitTermination()
    
  }//end main method
  
  def getGeoLocationCoordinates(status : twitter4j.Status) : (Double, Double) = {
    if (status.getGeoLocation != null) {
      return (status.getGeoLocation.getLatitude, status.getGeoLocation.getLongitude);
    }
    else {
      return (null.asInstanceOf[Double], null.asInstanceOf[Double])
    }
  }
  
  def getBoundingBoxCoordinates(status : twitter4j.Status) : (Double, Double) = {
    if (status != null && status.getPlace != null && status.getPlace.getBoundingBoxCoordinates != null) {
      return (status.getPlace.getBoundingBoxCoordinates.head.head.getLatitude, status.getPlace.getBoundingBoxCoordinates.head.head.getLongitude)
    }
    else {
      return (null.asInstanceOf[Double], null.asInstanceOf[Double])
    }
  }
   
}//end class

