package it.reti.spark.sentiment

import twitter4j.GeoLocation
import org.apache.spark.Logging



//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/**
 * Location class
 * @param userSelection: it is an integer according to which a specific area of interest is defined
 * It contains methods to check if tweet location is inside desired area of interest
 */
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////  
@SerialVersionUID(100L)
class Location(userSelection: Int) extends Serializable with Logging{
  
  
  //define the areaOfInterest according to userSelection
  private val areaOfInterest = new Array[twitter4j.GeoLocation](2)
  
  userSelection match{
    
    // London
    case 2 => {  areaOfInterest(0) = new GeoLocation(51.248325, -0.637189)  //bottom-left
                 areaOfInterest(1) = new GeoLocation(51.734844, 0.311755)} //top-right
    // Australia
    case 1 => {  areaOfInterest(0) = new GeoLocation(-42.565406, 112.894454)  //bottom-left
                 areaOfInterest(1) = new GeoLocation(-12.141625, 155.521404) }//top-right

    
    // USA
    case 3 => {  areaOfInterest(0) = new GeoLocation(23.357475, -126.549851)  //bottom-left
                 areaOfInterest(1) = new GeoLocation(49.178176, -66.960012) }//top-right
    
    // Lombardia
    case 4 => {  areaOfInterest(0) = new GeoLocation(44.973063, 8.488667)  //bottom-left
                 areaOfInterest(1) = new GeoLocation(46.5868, 10.756680) }//top-right
    
    // ALL OVER THE WORLD
    case default => {
                      /*<<< WARN >>>*/ logWarning("input error: ALL WORLD selected!")
                      areaOfInterest(0) = new GeoLocation(180, -180)  //bottom-left
                      areaOfInterest(1) = new GeoLocation(90, -90) }//top-right
      
                      
    
  
  }
  
  
  
  //.............................................................................................................
  /**
   * method that
   * @return selected areaOfInterest
   */
  def getAreaOfInterest = areaOfInterest

  
  
  
  //.............................................................................................................
  /**
   * location checker
   * @param tweet: is the tweet status to be checked
   * @return true if tweet location is inside desired area of interest
   */
  def checkLocation(tweet: twitter4j.Status): Boolean = {
    
     //if i have geoLocation value
     if(tweet.getGeoLocation !=null){
      
       if(  latitudeInBox(tweet.getGeoLocation.getLatitude) 
             && longitudeInBox(tweet.getGeoLocation.getLongitude)  ) true
       else false 
     }
     else{
       
       //if i have Place value
       if(tweet.getPlace !=null){
           if(  latitudeInBox(tweet.getPlace.getBoundingBoxCoordinates.head.head.getLatitude) 
                 && longitudeInBox(tweet.getPlace.getBoundingBoxCoordinates.head.head.getLongitude)  ) true
           else false 
       
       }else false
     }

  
  }// end checkLocation method //


  
  //.............................................................................................................
  /**
   * Latitude checker
   * @param latitudeToCheck 
   * @return true if it is in my areaOfInterest latitude range
   */
  def latitudeInBox(latitudeToCheck: Double): Boolean = {
    
    if ((latitudeToCheck >= areaOfInterest.head.getLatitude) && (latitudeToCheck <= areaOfInterest.last.getLatitude))
      true
    else
      false 
      
  }//end latitudeInBox method //



  
  

  //.............................................................................................................
 /**
   * Longitude checker
   * @param longitudeToCheck 
   * @return true if it is in my areaOfInterest longitude range
   */
  def longitudeInBox(longitudeToCheck: Double): Boolean = {
    
    if ((longitudeToCheck >= areaOfInterest.head.getLongitude) && (longitudeToCheck <= areaOfInterest.last.getLongitude))
      true
    else
      false 
  }//end latitudeInBox method //


  
  

}//end Location class //