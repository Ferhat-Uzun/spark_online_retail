package SparkRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapTransformation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setAppName("MapTransformation")
      .setMaster("local[4]")

    val sc = new SparkContext(conf)

    val retailRDD = sc.textFile("OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo"))

    retailRDD.take(3).foreach(println)

    case class CancelledPrice(isCancelled:Boolean, total:Double)

    var retailTotal =retailRDD.map(x =>{
      var isCancelled:Boolean = if(x.split(";")(0).startsWith("C")) true else false
      var total:Double = x.split(";")(3).toDouble*x.split(";")(5).replace(",",".").toDouble

      CancelledPrice(isCancelled,total)
    })

    retailTotal.take(5).foreach(println)

    retailTotal.map(x => (x.isCancelled,x.total))
      .reduceByKey((x,y) => x+y)
      .filter(x=> x._1)
      .map(x => x._2)
      .take(5)
      .foreach(println)


  }

}
