package it.reti.spark.sentiment

object Main {
  def main(args: Array[String]) {
    
    val app = args(0) match {
      case "batch" => {
        val fileName = "/user/maria_dev/Tutorials/OpFelicita/LOMBARDIA.20160405-125641.json"
        new TweetBatchApp(fileName)
      }
      case  "streaming" => {
        println("\nSelect country to spy: \n \t\t 1: Australia \n \t\t 2: London \n \t\t 3: Busto Arsizio \n \t\t 4: USA")
        val location = Console.readLine()
        new TweetStreamingApp(location)
      }
      case default => {
        println("Parametro errato, passare batch o streaming")
        null
      }
    }
    
    if (app != null) app.Run()
    
  }
}