
import java.util.UUID

//import PlayersRankingLoader.playersRowRDDtoCSV
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object GamesLoader {

  def UUIDLong: String => Long = s => UUID.nameUUIDFromBytes(s.getBytes()).getLeastSignificantBits.abs
  def rowRDDtoCSV(sc: SparkContext, pgnPath: String = Property.PGN_FILE): Unit = {


    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = PGNExtractTransform.pgnETtoRowRDD(sc, pgnPath)

    val df = spark.createDataFrame(gameTup, StructType(Property.gameSchema))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
      .set("spark.executor.memory", "4G")
      .set("spark.driver.memory", "4G")
    val sc = new SparkContext(conf)


//    PlayersRankingLoader.playersRowRDDtoCSV(sc, Property.PGN_FILE)


  }
}
