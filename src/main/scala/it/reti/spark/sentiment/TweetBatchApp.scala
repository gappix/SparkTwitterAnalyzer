package it.reti.spark.sentiment

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.when
  
/**
 * This class is an extension of TweetApp one.
 * It implements a specific Run method for batch data extraction from a file database.
 * It then uses upper-class methods for data elaboration and result storing.  
 */
class TweetBatchApp(fileNameAndPath : String) extends TweetApp(fileNameAndPath) {  

  /**
   * Run method OVERRIDED in order to fulfill batch app processing needing
   * It takes source filename (a JSON-row database with tweets data), extracts interesting infos packing 
   * everything in a single DataFrame.
   * This structure is then passed to upper-class "elaborate" method in order to retrieve sentiment evaluation.
   * Results are eventually stored into HIVE tables by invoking upper-class "storeDataFrameToHIVE" method.
   */
  override def Run() {
    //HIVE Context import
    val sqlContextHIVE = ContextHandler.getSqlContextHIVE
    import sqlContextHIVE.implicits._

    //function for unboxing bounding box structure in order to get Place Latitude information
    val extract_bounding_box_latitude = udf((box: Seq[Seq[Seq[Double]]]) => {
      box.head.head.last
    })
    
    //function for unboxing bounding box structure in order to get Place Longitude information
    val extract_bounding_box_longitude = udf((box: Seq[Seq[Seq[Double]]]) => {
      box.head.head.head
    })
    
    //function for unboxing bounding box structure in order to get Geo Local Latitude information
    val extract_geo_localization_latitude = udf((box: Seq[Double]) => {
      box.last
    })
    
    //function for unboxing bounding box structure in order to get Geo Local Longitude information
    val extract_geo_localization_longitude = udf (( box: Seq[Double]) =>{
      box.head
    })
    
    val inputTWEETS = sqlContextHIVE.read.json(fileNameAndPath) // Tweet json storage load
    val englishTWEETS = inputTWEETS.filter($"lang".equalTo("en"))     // Filtering based on language field
    
    //DataFrame is created by selecting interested fields from input DataFrame
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
    )
    
    //evaluate sentiment
    val elaboratedTweets = Elaborate(readyTWEETS)
    
    //store DataFrames into HIVE tables
    storeDataFrameToHIVE(elaboratedTweets.allTweets, elaboratedTweets.sentimentTweets)
  }
}