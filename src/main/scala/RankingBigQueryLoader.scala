import PlayersRanking.joinedRankingRDDsToDF
import Property.{BUCKET, DATASET}
import org.apache.spark.sql.SparkSession

object RankingBigQueryLoader {

  def rowRDDtoBigQuery(sparkSession: SparkSession, pgnPath: String, prItr: Int, onEvent: String): Unit = {


    val bucket = BUCKET
    sparkSession.conf.set("temporaryGcsBucket", bucket)


    val gamesRDD = PGNExtractTransform.pgnETtoTupleRDDs(
      sparkSession.sparkContext,
      pgnPath,
      PGNExtractTransform.rankingUnUsedDataFilter)


    val rankingDF = joinedRankingRDDsToDF(sparkSession, gamesRDD, prItr, onEvent)


    rankingDF
      .write
      .format("bigquery")
      .option("table", DATASET + ".ranking" + prItr.toString + onEvent)
      .mode("overwrite").save()


  }

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("parameters missing")
      return
    }


    val pgnPath = args(0)
    val prItr = args(1).toInt
    val onEvent = args(1)

    val sparkSession = SparkSession
      .builder()
      .appName("lichess")
      .getOrCreate()
    rowRDDtoBigQuery(sparkSession, pgnPath, prItr, onEvent)


  }


}
