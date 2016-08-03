package it.reti.spark.sentiment

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel





/*°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°*/
/**
 * This object stores processing DataFrames into correct HIVE tables
 */

class DataStorer(processingType: String) extends Serializable with Logging{
  
   
  
  //get sqlHIVE context and import methods for DataFrame/RDD conversion 
  private val sqlContextHIVE = ContextHandler.getSqlContextHIVE
  import sqlContextHIVE.implicits._
  
  

  
  
  
  //check output table creation
  val tableTweets =     "ademo_tweets_"    + processingType                 
  val tableSentiment =  "ademo_sentiment_"   + processingType         
  val saveLocation =  "/apps/hive/warehouse/"        
  
  

  
  val createTableTweets = "CREATE TABLE IF NOT EXISTS " +              
                          tableTweets +
                          " (tweet_id BIGINT, lang STRING, user_id BIGINT, user_name STRING, latitude DOUBLE, longitude DOUBLE, text STRING) " + 
                          " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " + 
                          " STORED AS ORC " +
                          """ TBLPROPERTIES ( "orc.compress"="SNAPPY" )""" /*+
                          """ location """" + saveLocation + """" """ */
                          
                          
  val createTableSent   = "CREATE TABLE IF NOT EXISTS " + 
                          tableSentiment + 
                          " (tweet_id BIGINT, sentiment_value DOUBLE, matched_words BIGINT, tweet_words BIGINT, confidency_value DOUBLE)" +
                          "  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " + 
                          " STORED AS ORC " +
                          """ TBLPROPERTIES ( "orc.compress"="SNAPPY" ) """ /*+
                          """ location """" + saveLocation + """" """*/
                           
  
  
  sqlContextHIVE.sql( createTableTweets )     
  sqlContextHIVE.sql( createTableSent )


  
  
  /*.................................................................................................................*/
  /**
   * method to store tweet infos into HIVE tweetTable
   * @param tweetDF: a DataFrame of elaborated tweets ready to be stored
   */
  def storeTweetsToHIVE (tweetDF: DataFrame ) = {
        

        /*<<INFO>>*/  logInfo("""Writing tweets into HIVE """" + tableTweets + """" table (through a direct save)""")
        tweetDF.persist().write.format("orc").mode("append").save(saveLocation + tableTweets)
        /*<<INFO>>*/  logInfo("Tweets written!")
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")
        tweetDF.show()
        tweetDF.unpersist()
        /*<<INFO>>*/  logInfo("-End of show-")
   
    
  }//end storeTweetsToHIVE method //
  
  
 
  
  /*.................................................................................................................*/
  /**
   * method to store sentiment infos into HIVE sentimentTable
   * @param sentimentDF: a DataFrame of elaborated sentiment values ready to be stored
   */
  def storeSentimentToHIVE (sentimentDF: DataFrame) = {
        
    
        /*<<INFO>>*/ logInfo("""Writing sentiment results into HIVE """" + tableSentiment + """" table (through a direct save)""") 
        sentimentDF.persist().write.format("orc").mode("append").save(saveLocation + tableSentiment)
        /*<<INFO>>*/  logInfo("Sentiment written!")
        /*<<INFO>>*/  logInfo("The following content has successfully been stored:")
        sentimentDF.show()
        sentimentDF.unpersist()
        /*<<INFO>>*/  logInfo("-End of show-")
        
 
  }//end storeSentimentToHIVE method //
  

  
  
}//end DataStorer class //