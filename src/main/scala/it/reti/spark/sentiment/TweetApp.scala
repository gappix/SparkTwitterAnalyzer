package it.reti.spark.sentiment

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.reflect.runtime.universe
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.Logging



/*°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°*/
/**
 *   This abstract class contains all main methods for processing execution.
 *   It must be extended implementing the Run method according to desired execution.
 *   Elaboration and storing methods are, on the contrary, common for every purpose.   
 */
abstract class TweetApp(processingType : String) extends Serializable with Logging{

  
  
  
  //data storer object
  val myDataStorer = new DataStorer(processingType)
  
  
  
  
  
  
  /*.................................................................................................................*/
  /** 
  * Method TRAIT.
  * MUST BE OVERRIDED in class extension.
  */
  def Run()
  
  
  
  
  
  
  
  /*.................................................................................................................*/
  /**
   * Method which elaborates tweet DataFrames evaluating sentiment values.
   * It  joins tweets with an "Hedonometer dictionary" which assign to each word an happiness value. 
   * All values are then averaged obtaining an approximate sentiment value for each tweet message.
   * 
   * @param allTWEETS	DataFrame with all tweet potentially useful fields
   * @return a tuple containing 2 DataFrames: processed tweets and sentiment evaluations
   */
  def Elaborate( allTWEETS : DataFrame) : {val allTweets : DataFrame; val sentimentTweets : DataFrame} = {
    
    
    
    //get sqlHIVE context and import methods for DataFrame/RDD conversion 
    val sqlContextHIVE = ContextHandler.getSqlContextHIVE
    import sqlContextHIVE.implicits._
    
    //getting the hedonometer
    val hedoDF = Hedonometer.getHedonometer
    
    
    
    /*------------------------------------------------*
     * UDF definitions
     *------------------------------------------------*/
    
    //confidency field value evaluator for each row
    val confidencyValue = udf( (matched_words: Double, tweet_words: Double) =>{  matched_words/tweet_words  })
    
    
    //sanitization by lower case and regular expression (only dictionary word extracted)
    val sanitizeTweet = udf (( word: String) =>{
                                                  val regularExpression = "\\w+(\'\\w+)?".r 
                                                  val sanitizedWord = regularExpression.findFirstIn(word.toLowerCase)
                                                  val emptyWord = ""
                                                  sanitizedWord match 
                                                  {
                                                      case None            => emptyWord
                                                      case Some(something) => something     }
                                               })
    
                               
    /*---------------------------------------------------*
     * DataFrame transformations 
     *---------------------------------------------------*/                               
                               
    /*
     * Selecting desired fields:
     * if available, take GeoLocation infos (which are most accurate)
     * otherwise take Place.BoundingBox ones
     */    
    val readyTWEETS = allTWEETS.select(
        $"tweet_id",
        $"lang",
        $"user_id",
        $"user_name",
        when($"gl_latitude".isNull, $"bb_latitude").otherwise($"gl_latitude").as("latitude"),
        when($"gl_longitude".isNull, $"bb_longitude").otherwise($"gl_longitude").as("longitude"),
        $"text"
        )
        
     
    

    
    //explode each tweet by having one word for each row
    val explodedTWEETS = readyTWEETS
                       .select($"tweet_id", $"text")//only tweet_id and lowered text needed
                       .explode("text", "word"){text: String => text.split(" ")}//explode  text(n-words)(1-row) field in 
                                                                                //         word(1-word)(n-rows) one

    
 
    
    
    //sanitize words by udf 
    val sanitizedTWEETS = explodedTWEETS
                           .select($"tweet_id", sanitizeTweet(explodedTWEETS("word")).as("word"))
    

    //count original tweet words
    val wordCountByTweetDF = sanitizedTWEETS
                               .groupBy("tweet_id").count()
                               .withColumnRenamed("count","tweet_words")
    


   /*---------------------------------------------------*
    * Sentiment evaluation
    *---------------------------------------------------*/
    
    /*<<INFO>>*/ logInfo("Joining Dataframes " + hedoDF.toString() + " and " + sanitizedTWEETS.toString())
                               
    //joining tweets with Hedonometer dictionary                           
    val sentimentTWEETS = hedoDF
                               .join(sanitizedTWEETS, hedoDF("dictionary") === sanitizedTWEETS("word"), "inner")
                               .groupBy("tweet_id")
                               .agg( "sentiment_value"  -> "avg",
                                     "word"             -> "count" )
                               .withColumnRenamed("avg(sentiment_value)","sentiment_value")
                               .withColumnRenamed("count(word)","matched_words")
    
    
    
    //pack results into a new DataFrame
    val sentimentConfidencyTWEETS = sentimentTWEETS
                                      .join( wordCountByTweetDF, "tweet_id")
                                      .select( $"tweet_id",
                                               $"sentiment_value",
                                               $"matched_words",
                                               $"tweet_words",
                                               confidencyValue($"matched_words", $"tweet_words").as("confidency_value")
                                      )
    
          
    //packing returning DataFrames into single struct      
    new {
      val allTweets = readyTWEETS
      val sentimentTweets = sentimentConfidencyTWEETS
    }
    
    
  }
  //end elaborate method //
  
  
  
  
  
  /*.................................................................................................................*/
  /**
   * Method which stores DataFrames with elaborated values into HIVE tables 
   * 
   * @tweetProcessedDF	DataFrame containing already processed tweets with final values
   * @sentimentDF				DataFrame containing tweet sentiment and confidency evaluation 
   * 
   */
  def storeDataFrameToHIVE ( tweetProcessedDF: DataFrame,  sentimentDF: DataFrame) {
     
    
    
    
    //call the DataStore object passing the Dataframe to store
    myDataStorer.storeTweetsToHIVE(tweetProcessedDF)
    myDataStorer.storeSentimentToHIVE(sentimentDF)


    
    
  }//end storeDataFrameToHIVE method //
  
  
  
  
  
}//end  TweetApp Class //