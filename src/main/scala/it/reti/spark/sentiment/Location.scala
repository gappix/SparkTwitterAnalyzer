package it.reti.spark.sentiment

import twitter4j.GeoLocation

/**=================================================================================================================
 * Class Location for all location needed methods
 *=================================================================================================================*/
@SerialVersionUID(100L)
class Location(userSelection: Int) extends Serializable {
  
  private val areaOfInterest = new Array[GeoLocation](2)
  
  //define the areaOfInterest according to userSelection
  userSelection match{
    
    // London
    case 2 => {  areaOfInterest(0) = new GeoLocation(51.248325, -0.637189)  //bottom-left
                 areaOfInterest(1) = new GeoLocation(51.734844, 0.311755)} //top-right
    // Australia
    case 1 => {  areaOfInterest(0) = new GeoLocation(-42.565406, 112.894454)  //bottom-left
                 areaOfInterest(1) = new GeoLocation(-12.141625, 155.521404) }//top-right
    // Busto Arsizio
    case 3 => {  areaOfInterest(0) = new GeoLocation(45.482755, 8.761141)  //bottom-left
                 areaOfInterest(1) = new GeoLocation(45.692082, 8.995287) }//top-right
    
    // USA
    case 4 => {  areaOfInterest(0) = new GeoLocation(23.357475, -126.549851)  //bottom-left
                 areaOfInterest(1) = new GeoLocation(49.178176, -66.960012) }//top-right
  
  }//end of get_Location
  
  /*
   * area of interest getter
   */
  def getAreaOfInterest = areaOfInterest

/*
 * location checker
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

  
}




/*
* Check if Latitude is in my areaOfInterest range
*/
  
def latitudeInBox(latitudeToCheck: Double): Boolean = {
  
  if ((latitudeToCheck >= areaOfInterest.head.getLatitude) && (latitudeToCheck <= areaOfInterest.last.getLatitude))
    true
  else
    false 
}//end latitudeInBox





/*
* check if Longitude is in my areaOfInterest range
*/
  
def longitudeInBox(longitudeToCheck: Double): Boolean = {
  
  if ((longitudeToCheck >= areaOfInterest.head.getLongitude) && (longitudeToCheck <= areaOfInterest.last.getLongitude))
    true
  else
    false 
}//end latitudeInBox



}//end Location class
	 


