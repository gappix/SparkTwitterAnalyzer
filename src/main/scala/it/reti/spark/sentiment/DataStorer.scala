package it.reti.spark.sentiment

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext



//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * This object stores processing DataFrames into correct HIVE tables
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class DataStorer(processingType: String) extends Serializable with Logging{
  
   
  
  //get sqlHIVE context and import methods for DataFrame/RDD conversion 
  private val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  import sqlContextHIVE.implicits._
  

  
  //check output table creation
  val tableTweets = "tweets_processed_" + processingType 
  val tableSentiment = "tweets_sentiment_"  + processingType 

  sqlContextHIVE.sql("create table if not exists " 
                     + tableTweets    
                     + " (`tweet_id` bigint, `lang` string, `user_id` bigint, `user_name` string, `latitude` double ,`longitude` double, `text` string) stored as orc"
                     )
  sqlContextHIVE.sql("create table if not exists " 
                     + tableSentiment 
                     + " (`tweet_id` bigint, `sentiment_value` double, `matched_words` bigint, `tweet_words` bigint, `confidency_value` double) stored as orc"
                     )
  
  
  
  
  
  //.................................................................................................................
  /**
   * method to store tweet infos into HIVE tweetTable
   * @param tweetDF: a DataFrame of elaborated tweets ready to be stored
   */
  def storeTweetsToHIVE (tweetDF: DataFrame) = {
        
        /*<<INFO>>*/  logInfo("Writing tweets into HIVE table:")
        tweetDF.cache().write.format("orc").mode(SaveMode.Append).save("/apps/hive/warehouse/" + tableTweets)
        /*<<INFO>>*/  logInfo("...")
        tweetDF.show()
        tweetDF.unpersist()
    
  }//end storeTweetsToHIVE method //
  
  
 
  
    //.................................................................................................................
  /**
   * method to store sentiment infos into HIVE sentimentTable
   * @param sentimentDF: a DataFrame of elaborated sentiment values ready to be stored
   */
  def storeSentimentToHIVE (sentimentDF: DataFrame) = {
        
        /*<<INFO>>*/ logInfo("Writing sentiment results into HIVE table:") 
        sentimentDF.cache.write.format("orc").mode(SaveMode.Append).save("/apps/hive/warehouse/" + tableSentiment)
        /*<<INFO>>*/  logInfo("...")
        sentimentDF.show()
        sentimentDF.unpersist()
        
        
  }//end storeSentimentToHIVE method //
  

  
  
}//end DataStorer class //