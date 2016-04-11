package it.reti.spark.twitter
import org.apache.spark.{SparkConf, SparkContext}

object Program {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("my-app")
      .setMaster("local")

    val sc = new SparkContext(conf)
  }
}