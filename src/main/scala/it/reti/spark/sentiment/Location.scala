package it.reti.spark.sentiment

import org.apache.log4j.Logger

import twitter4j.GeoLocation

/**
 * Location class
 * @param userSelection: it is an integer according to which a specific area of interest is defined
 * It contains methods to check if tweet location is inside desired area of interest
 */
@SerialVersionUID(3145638254L)
class Location(userSelection: Int) extends Serializable {
  private val logger = Logger.getLogger(getClass.getName)
  
  final val AUSTRALIA = 1
  final val LONDON = 2
  final val BUSTO_ARSIZIO = 3
  final val USA = 4
  
  //define the areaOfInterest according to userSelection
  private val areaOfInterest = new Array[twitter4j.GeoLocation](2)
  
  userSelection match{
    case LONDON => {
      areaOfInterest(0) = new GeoLocation(51.248325, -0.637189) //bottom-left
      areaOfInterest(1) = new GeoLocation(51.734844, 0.311755) //top-right
    }
    case AUSTRALIA => {
      areaOfInterest(0) = new GeoLocation(-42.565406, 112.894454) //bottom-left
      areaOfInterest(1) = new GeoLocation(-12.141625, 155.521404) //top-right
    }
    case BUSTO_ARSIZIO => { 
      areaOfInterest(0) = new GeoLocation(45.0, 8.0) //bottom-left
      areaOfInterest(1) = new GeoLocation(46.0, 9.0) //top-right
    }
    case USA => {
      areaOfInterest(0) = new GeoLocation(23.357475, -126.549851) //bottom-left
      areaOfInterest(1) = new GeoLocation(49.178176, -66.960012) //top-right
    }
    case default => {
      logger.warn("input error: ALL WORLD selected!")
      areaOfInterest(0) = new GeoLocation(180, -180) //bottom-left
      areaOfInterest(1) = new GeoLocation(90, -90) //top-right
    }
  }

  /**
   * Method that return selected area of interest
   * @return the selected area of interest
   */
  def getAreaOfInterest = areaOfInterest

  /**
   * Method that checks the location contained in a tweet object.
   * @param tweet the tweet status to be checked
   * @return true if tweet location is inside desired area of interest
   */
  def checkLocation(tweet: twitter4j.Status): Boolean = {
    // if I have geoLocation value
    if (tweet.getGeoLocation != null) {
      val latitude = tweet.getGeoLocation.getLatitude
      val longitude = tweet.getGeoLocation.getLongitude
      return latitudeInBox(latitude) && longitudeInBox(longitude)
    }
     
    // if I have Place value
    if (tweet.getPlace != null) {
      val latitude = tweet.getPlace.getBoundingBoxCoordinates.head.head.getLatitude
      val longitude = tweet.getPlace.getBoundingBoxCoordinates.head.head.getLongitude
      return latitudeInBox(latitude) && longitudeInBox(longitude)
    }
    
    // otherwise (no geoLocation and no Place in tweet)
    return false
  }

  /**
   * Method that checks if a specified latitude is in the area of interest.
   * @param latitudeToCheck the latitude to check as a Double value 
   * @return true if it is in my areaOfInterest latitude range
   */
  def latitudeInBox(latitudeToCheck: Double): Boolean = {
    return (latitudeToCheck >= areaOfInterest.head.getLatitude) && (latitudeToCheck <= areaOfInterest.last.getLatitude)
  }

 /**
   * Method that checks if a specified longitude is in the area of interest.
   * @param longitudeToCheck the longitude to check as a Double value
   * @return true if it is in my areaOfInterest longitude range
   */
  def longitudeInBox(longitudeToCheck: Double): Boolean = {
    return (longitudeToCheck >= areaOfInterest.head.getLongitude) && (longitudeToCheck <= areaOfInterest.last.getLongitude) 
  }
  
}