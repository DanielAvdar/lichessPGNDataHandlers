

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row




object GamesLoader {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[9]").setAppName("lichess")
    val sc = new SparkContext(conf)
    val gameRows=PGNExtractTransform.pgnETLtoRowRDD(sc)
    gameRows.foreach(println)



  }

}
