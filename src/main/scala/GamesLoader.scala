

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row




object GamesLoader {


  def main(args: Array[String]): Unit = {
    val gameRows=PGNExtractTransform.pgnETLtoRowRDD()



  }

}
