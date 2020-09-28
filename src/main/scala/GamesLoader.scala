
import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


object GamesLoader {
  val DIRECTORY = "C:\\tmp_test\\"//todo
  val PGN_FILE: String = DIRECTORY + "lichess_db_standard_rated_2013-01.pgn.bz2"//todo
  val PGN_FILE2: String = DIRECTORY + "lichess_db_standard_rated_2014-07.pgn.bz2"//todo
  val PGN_FILE3:String = DIRECTORY + "lichess_db_standard_rated_2017-04.pgn.bz2"//todo

  val csvPath="D:\\temporary\\tmp.csv"//todo replace
  val schemaString: String = "Id Event WightName BlackName Winner WightRating BlackRating" +
    " ECO Opening time-control Date Time Termination GamePlay"
  val schema: Array[String] = schemaString.split(" ")
  val schemaSeq: Seq[StructField] = Seq("Id", "Event", "WightName", "BlackName", "Winner",
    "WightRating", "BlackRating", "ECO", "Opening", "time-control"
    , "Date", "Time", "Termination", "GamePlay").map(feild => StructField(feild, StringType, nullable = false))



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
      .set("spark.executor.memory","4G")
      .set("spark.driver.memory","4G")
    val sc = new SparkContext(conf)

    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = PGNExtractTransform.pgnETLtoRowRDD(sc,PGN_FILE3)

//-Xms6g -Xmx10g
    val df = spark.createDataFrame(gameTup, StructType(schemaSeq))
//    val tmp=df.take(50)
//    println(tmp.toSeq.toString())

    df.write.format("csv").mode("overwrite").save(csvPath)

    //    val df =
    //    df.write.format("csv").save(filepath)


  }
  def tmppp(row: Row):Unit={

    for(i <-row.toSeq)
    {println(i)}

  }

}
