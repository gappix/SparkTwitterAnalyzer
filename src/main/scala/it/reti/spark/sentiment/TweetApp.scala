package it.reti.spark.sentiment

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

//Import all necessed packages
import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class Hedo(
    dictionary: String,
    rank: Int,
    sentiment_value: Float,
    other1: Float,
    other2: String,
    other3: String,
    other4: String)
  
object TweetApp {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("OPFelicitaS")
      .setMaster("local")
      
    val sc = new SparkContext(conf)
    
    //spark-sql context creation from sc
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContextHIVE = new org.apache.spark.sql.hive.HiveContext(sc)
    
    //import methods for DataFrame/RDD conversion
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._
    
    /**
    ------------------------------------------ TWEET PROCESSING -------------------------------------------
    */
    
    //Tweet json storage load
    val inputTWEETS = sqlContextHIVE.read.json("/user/maria_dev/Tutorials/OpFelicita/LOMBARDIA.20160405-125641.json")
    
    
    //Filtering based on language field
    val englishTWEETS = inputTWEETS.filter($"lang".equalTo("en"))
    
    
    
    
    
    
    ////////////////////// UDFs to retrieve decapsulated coordinates ////////////////////////////////////////////////////////
    
    val extract_bounding_box_latitude = udf (( box: Seq[Seq[Seq[Double]]]) =>{
    
                val latitude = box.head.head.last
                latitude
    })
    
    //////////////////////////////////
    
    val extract_bounding_box_longitude = udf (( box: Seq[Seq[Seq[Double]]]) =>{
    
                val longitude = box.head.head.head
                longitude
    })
    
    //////////////////////////////////////////////////
    
    val extract_geo_localization_latitude = udf (( box: Seq[Double]) =>{
    
                val latitude = box.last
                latitude
    })
    
    ///////////////////////////////////
    
    val extract_geo_localization_longitude = udf (( box: Seq[Double]) =>{
    
                val longitude = box.head
                longitude
    })
    
    /////////////////////////////////////////////////////////////////////////////////////////////////////
    
    
    
                
    val readyTWEETS = englishTWEETS.select( 
                                                   
                                        $"id".as("tweet_id"), 
                                        $"lang", 
                                        $"user.id".as("user_id"), 
                                        $"user.name".as("user_name"),
    
                                        //latitude extraction from the correct field
                                        when($"coordinates".isNull, extract_bounding_box_latitude(englishTWEETS("place.bounding_box.coordinates")))
                                        .otherwise(extract_geo_localization_latitude(englishTWEETS("coordinates.coordinates")))
                                        .as("latitude"),
    
                                        //longitude extraction from the correct field
                                        when($"coordinates".isNull, extract_bounding_box_longitude(englishTWEETS("place.bounding_box.coordinates")))
                                        .otherwise(extract_geo_localization_longitude(englishTWEETS("coordinates.coordinates")))
                                        .as("longitude"),
    
                                        $"text"
                                                                                       )// end select
                                                   
    
    /*
    
    
    readyTWEETS :                                            [           TWEET_ID                                       long                 | 
                                                                                                   LANG                                                            chararray         | (en)
                                    USER_ID                                              long                 | 
                                    USER_NAME                                      chararray         | 
                                    LATITUDE                                           float                | 
                                    LONGITUDE                                       float                | 
                                    TEXT                                                    chararray         ] (in english)
    
    
    */
    
    
    
    
    
    
    
    
    
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
           sanitizedWord match{
                      case None                               => emptyWord
                      case Some(something)=> something
           }
     })
    
    /////////////////////////////////////////////////
    
    val sanitizedTWEETS = explodedTWEETS
           .select($"tweet_id", sanitizeTweet(explodedTWEETS("word")).as("word"))
    

    
    val wordCountByTweetDF = sanitizedTWEETS
             .groupBy("tweet_id").count()
             .withColumnRenamed("count","tweet_words")
    

    //------------------------------------- HEDONOMETER LOADING -----------------------------------------------------
    

    //loading from txt file
    val inputHEDONOMETER = sc.textFile("/user/maria_dev/Tutorials/OpFelicitaS/hedonometerNOHEADER.txt")
    
    //splitting fields
    val hedonometerDF = inputHEDONOMETER
         //splitto quando trovo degli spazi
         .map(_.split("\t")) 
         //associo la case Class HEDO con la relativa struttura
         .map( p => Hedo( p(0), p(1).toInt, p(2).toFloat, p(3).toFloat, p(4), p(5), p(6))) 
         //trasformo in DataFrame l'RDD risultante
         .toDF

    //-------------------------------- JOIN AND SENTIMENT EVALUATION ------------------------------

    val sentimentTWEETS = sanitizedTWEETS
         .join(hedonometerDF, $"word" === $"dictionary")
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
    
    sentimentConfidencyTWEET.registerTempTable("tweet_sentiment_TEMP")
    
    sqlContextHIVE.sql("CREATE TABLE bubba AS SELECT * FROM tweet_sentiment_TEMP")
    //sqlContextHIVE.sql("INSERT INTO TABLE bubba SELECT * FROM tweet_sentiment_TEMP")
  }
}