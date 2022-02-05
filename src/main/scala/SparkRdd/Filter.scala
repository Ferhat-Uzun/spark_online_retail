package SparkRdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("online_retail").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val retailRDD = sc.textFile("OnlineRetail.csv").mapPartitionsWithIndex(
      (idx, iter) => if (idx==0) iter.drop(1) else iter
    )

    retailRDD.take(10).foreach(println) //Raw Data

    val count = retailRDD.count()
    println(count)// Count of Data line

    println("\n\n********Filter Unit Quantity Less Than 30********")

    val filteredData=retailRDD.filter(x => x.split(";")(3).toInt < 30)
    filteredData.take(5).foreach(println)
    println(filteredData.count())


    println("\n\n********Filter if containing COFFEE and unit price expensive than 20.0")
    val filtered= retailRDD.filter(x=>
      x.split(";")(2).contains("COFFEE")
      &&
      x.split(";")(5).trim.replace(",",".").toFloat>20.0F)
    filtered.take(5).foreach(println)

  }
}
