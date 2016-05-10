package it.reti.spark.sentiment

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe
import org.apache.spark.sql.hive.HiveContext
  
abstract class TweetApp(runParam : String) extends Serializable {
  def Run()
  
  def Elaborate(allTWEETS : DataFrame) : {val allTweets : DataFrame; val sentimentTweets : DataFrame} = {
    val sc = new SparkContext(new SparkConf().setAppName("OPFelicitaS").setMaster("local[*]"))
    val sqlContext = new SQLContext(sc)
    val sqlContextHIVE = new HiveContext(sc)
     
    //import methods for DataFrame/RDD conversion  
    import sqlContext.implicits._
    
    val readyTWEETS = allTWEETS.select(
        $"tweet_id",
        $"lang",
        $"user_id",
        $"user_name",
        when($"gl_latitude".isNull, $"bb_latitude").otherwise($"gl_latitude").as("latitude"),
        when($"gl_longitude".isNull, $"bb_longitude").otherwise($"gl_longitude").as("longitude"),
        $"text");
                                                   
    val explodedTWEETS = readyTWEETS
         //only tweet_id and lowered text needed
         .select($"tweet_id", $"text")
         //explode text (n-words)(1-row) field in word(1-word)(n-rows) one
         .explode("text", "word"){text: String => text.split(" ")}
    
    
    //////////////////////////////////////////////////
    val sanitizeTweet = udf (( word: String) =>{
        val regularExpression = "\\w+(\'\\w+)?".r 
        val sanitizedWord = regularExpression.findFirstIn(word.toLowerCase)
        val emptyWord = ""
        sanitizedWord match {
            case None            => emptyWord
            case Some(something) => something
        }
    })
    
    /////////////////////////////////////////////////
    
    val sanitizedTWEETS = explodedTWEETS
           .select($"tweet_id", sanitizeTweet(explodedTWEETS("word")).as("word"))
    

    
    val wordCountByTweetDF = sanitizedTWEETS
             .groupBy("tweet_id").count()
             .withColumnRenamed("count","tweet_words")
    

    //------------------------------------- HEDONOMETER LOADING -----------------------------------------------------
    val myHedonometer = new Hedonometer(sc)
    
    val sentimentTWEETS = sanitizedTWEETS
         .join(myHedonometer.getHedonometer, $"word" === $"dictionary")
         .groupBy("tweet_id")
         .agg( "sentiment_value"  -> "avg",
               "word"             -> "count" )
         .withColumnRenamed("avg(sentiment_value)","sentiment_value")
         .withColumnRenamed("count(word)","matched_words")
    
    ////////////////////////////////
    
    val confidencyValue = udf( (matched_words: Double, tweet_words: Double) => matched_words/tweet_words)
    
    val sentimentConfidencyTWEET = sentimentTWEETS
          .join( wordCountByTweetDF, "tweet_id")
          .select( $"tweet_id",
                   $"sentiment_value",
                   $"matched_words",
                   $"tweet_words",
                   confidencyValue($"matched_words", $"tweet_words").as("confidency_value")
          )
    
    new {
        val allTweets = readyTWEETS
        val sentimentTweets = sentimentConfidencyTWEET
    }
  }
}