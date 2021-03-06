package it.reti.spark.samples

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.net.Socket

class SocketReceiver(port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  var serverSocket : ServerSocket = null
  
  def onStart() {
    serverSocket = new ServerSocket(port)
    
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() {
        while (!isStopped) {
          receive()
        }
      }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
    serverSocket.close()
    serverSocket = null
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    try {
      // Until stopped or connection broken continue reading
      val socket : Socket = serverSocket.accept()
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
      
      var userInput: String = null
      do { 
        userInput = reader.readLine()
        if (userInput != null) store(userInput)
      }
      while(!isStopped && userInput != null)
      
      socket.close()
     } catch {
       case e: java.net.ConnectException =>
         // restart if could not connect to server
         restart("Error connecting to port " + port, e)
       case t: Throwable =>
         // restart if there is any other error
         restart("Error receiving data", t)
    }
  }
}