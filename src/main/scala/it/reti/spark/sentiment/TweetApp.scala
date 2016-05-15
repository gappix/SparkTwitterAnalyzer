package it.reti.spark.sentiment

import scala.reflect.runtime.universe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.when

/**
 *   This abstract class contains all main methods for processing execution.
 *   It must be extended implementing the Run method according to desired execution.
 *   Elaboration and storing methods are, on the contrary, common for every purpose.   
 */
@SerialVersionUID(8221842361L)
abstract class TweetApp(runParam : String) extends Serializable {
  /** 
   * Method trait that executes tweet analysis.
   * It must be overriden in class extension.
   */
  def Run()
  
  /**
    * Method which elaborates tweet DataFrames evaluating sentiment values.
    * It  joins tweets with an "Hedonometer dictionary" which assign to each word an happiness value. 
    * All values are then averaged obtaining an approximate sentiment value for each tweet message.
    * 
    * @param allTWEETS	DataFrame with all tweet potentially useful fields
    * @return a tuple containing 2 DataFrames: processed tweets and sentiment evaluations
    */
  def Elaborate(allTWEETS : DataFrame): { val allTweets : DataFrame; val sentimentTweets : DataFrame } = {
    val sqlContextHIVE = ContextHandler.getSqlContextHIVE //get sqlHIVE context
    
    import sqlContextHIVE.implicits._
    val myHedonometer = new Hedonometer // load hedonometer matrix
    
    //confidency field value evaluator for each row
    val confidencyValue = udf((matched_words: Double, tweet_words: Double) => matched_words/tweet_words)
    
    //sanitization by lower case and regular expression (only dictionary word extracted)
    val sanitizeTweet = udf (( word: String) => {
      val regularExpression = "\\w+(\'\\w+)?".r 
      val sanitizedWord = regularExpression.findFirstIn(word.toLowerCase)
      val emptyWord = ""
      sanitizedWord match {
        case None            => emptyWord
        case Some(something) => something
      }
    })
    
    // Selecting desired fields:
    // if available, take GeoLocation infos (which are most accurate) otherwise take Place.BoundingBox ones   
    val readyTWEETS = allTWEETS.select(
      $"tweet_id",
      $"lang",
      $"user_id",
      $"user_name",
      when($"gl_latitude".isNull, $"bb_latitude").otherwise($"gl_latitude").as("latitude"),
      when($"gl_longitude".isNull, $"bb_longitude").otherwise($"gl_longitude").as("longitude"),
      $"text"
    );
    
    // explode each tweet by having one word for each row
    val explodedTWEETS = readyTWEETS
     .select($"tweet_id", $"text") //only tweet_id and lowered text needed
     .explode("text", "word"){ text: String => text.split(" ") } //explode text(n-words)(1-row) field in word(1-word)(n-rows) one

    // sanitize words by udf 
    val sanitizedTWEETS = explodedTWEETS.select($"tweet_id", sanitizeTweet(explodedTWEETS("word")).as("word"))
    
    // count original tweet words
    val wordCountByTweetDF = sanitizedTWEETS.groupBy("tweet_id").count().withColumnRenamed("count","tweet_words")
                               
    // joining tweets with Hedonometer dictionary                           
    val sentimentTWEETS = sanitizedTWEETS
      .join(myHedonometer.getHedonometer, $"word" === $"dictionary")
      .groupBy("tweet_id")
      .agg( "sentiment_value"  -> "avg",
            "word"             -> "count" )
      .withColumnRenamed("avg(sentiment_value)","sentiment_value")
      .withColumnRenamed("count(word)","matched_words")
    
    // pack results into a new DataFrame
    val sentimentConfidencyTWEET = sentimentTWEETS
      .join( wordCountByTweetDF, "tweet_id")
      .select($"tweet_id",
              $"sentiment_value",
              $"matched_words",
              $"tweet_words",
              confidencyValue($"matched_words", $"tweet_words").as("confidency_value")
      )
    
    //packing returning DataFrames into single struct      
    new {
        val allTweets = readyTWEETS
        val sentimentTweets = sentimentConfidencyTWEET
    }
  }

  /**
   * Method which stores DataFrames with elaborated values into HIVE tables 
   * 
   * @param tweetProcessedDF	DataFrame containing already processed tweets with final values
   * @param sentimentDF				DataFrame containing tweet sentiment and confidency evaluation 
   */
  def storeDataFrameToHIVE (tweetProcessedDF: DataFrame,  sentimentDF: DataFrame) {  
    //get sqlHIVE context and import methods for DataFrame/RDD conversion 
    val sqlContextHIVE = ContextHandler.getSqlContextHIVE
    
    //print DataFrame to output
    tweetProcessedDF.show()
    sentimentDF.show()
    
    //store tempTables
    tweetProcessedDF.registerTempTable("processed_tweets_TEMP")
    sentimentDF.registerTempTable("tweet_sentiment_TEMP")
    
    //querying HIVE to store tempTable contents
    sqlContextHIVE.sql("CREATE TABLE IF NOT EXISTS tweets_processed (`tweet_id` bigint, `lang` string, `user_id` bigint, `user_name` string, `latitude` float, `longitude` float, `text` string) STORED AS ORC")
    sqlContextHIVE.sql("CREATE TABLE IF NOT EXISTS tweets_sentiment (`tweet_id` bigint, `sentiment_value` float, `matched_words` int, `tweet_words` int, `confidency_value` float) STORED AS ORC")
    sqlContextHIVE.sql("INSERT INTO TABLE tweets_processed SELECT * FROM processed_tweets_TEMP")
    sqlContextHIVE.sql("INSERT INTO TABLE tweets_sentiment SELECT * FROM tweet_sentiment_TEMP") 
  }
}