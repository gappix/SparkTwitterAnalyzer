package it.reti.spark.sentiment

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe
import org.apache.spark.sql.hive.HiveContext
  
class TweetBatchApp(runParam : String) extends TweetApp(runParam) {  
  override def Run() {
    val sc = new SparkContext(new SparkConf().setAppName("OPFelicitaS").setMaster("local[*]"))
    val sqlContext = new SQLContext(sc)
    val sqlContextHIVE = new HiveContext(sc)
      
    //import methods for DataFrame/RDD conversion
    import sqlContext.implicits._
    
    // Tweet json storage load
    val inputTWEETS = sqlContextHIVE.read.json(runParam)
    // Filtering based on language field
    val englishTWEETS = inputTWEETS.filter($"lang".equalTo("en"))
    
    val extract_bounding_box_latitude = udf((box: Seq[Seq[Seq[Double]]]) => {
      box.head.head.last
    })
    
    val extract_bounding_box_longitude = udf((box: Seq[Seq[Seq[Double]]]) => {
      box.head.head.head
    })
    
    val extract_geo_localization_latitude = udf((box: Seq[Double]) => {
      box.last
    })
    
    val extract_geo_localization_longitude = udf (( box: Seq[Double]) =>{
      box.head
    })
    
    val readyTWEETS = englishTWEETS.select( 
        $"id".as("tweet_id"), 
        $"lang", 
        $"user.id".as("user_id"), 
        $"user.name".as("user_name"),
        when($"place.bounding_box.coordinates".isNotNull, extract_bounding_box_latitude(englishTWEETS("place.bounding_box.coordinates"))).as("bb_latitude"),
        when($"coordinates.coordinates".isNotNull, extract_geo_localization_latitude(englishTWEETS("coordinates.coordinates"))).as("gl_latitude"),
        when($"place.bounding_box.coordinates".isNotNull, extract_bounding_box_longitude(englishTWEETS("place.bounding_box.coordinates"))).as("bb_longitude"),
        when($"coordinates.coordinates".isNotNull, extract_geo_localization_longitude(englishTWEETS("coordinates.coordinates"))).as("gl_longitude"),
        $"text"
    )// end select
    
    val elaboratedTweets = Elaborate(readyTWEETS)

    //registering temp table to cache data
    elaboratedTweets.allTweets.show()
    elaboratedTweets.sentimentTweets.show()
    
    elaboratedTweets.allTweets.registerTempTable("processed_tweets_TEMP")
    elaboratedTweets.sentimentTweets.registerTempTable("tweet_sentiment_TEMP")
    
    //querying HIVE to store tempTable contents
    sqlContextHIVE.sql("CREATE TABLE IF NOT EXISTS tweets_processed_2 (`tweet_id` bigint, `lang` string, `user_id` bigint, `user_name` string, `latitude` float, `longitude` float, `text` string) STORED AS ORC")
    sqlContextHIVE.sql("CREATE TABLE IF NOT EXISTS tweets_sentiment_2 (`tweet_id` bigint, `sentiment_value` float, `matched_words` int, `tweet_words` int, `confidency_value` float) STORED AS ORC")
    sqlContextHIVE.sql("INSERT INTO TABLE tweets_processed_2 SELECT * FROM processed_tweets_TEMP")
    sqlContextHIVE.sql("INSERT INTO TABLE tweets_sentiment_2 SELECT * FROM tweet_sentiment_TEMP")

  }
}