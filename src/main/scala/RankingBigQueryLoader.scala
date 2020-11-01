import PlayersRanking.joinedRankingRDDsToDF
import Property.{BUCKET, DATASET}
import org.apache.spark.sql.SparkSession

object RankingBigQueryLoader {

  def rowRDDtoBigQuery(sparkSession: SparkSession, pgnPath: String, prItr: Int): Unit = {


    val bucket = BUCKET
    sparkSession.conf.set("temporaryGcsBucket", bucket)


    val gamesRDD = PGNExtractTransform.pgnETtoTupleRDDs(
      sparkSession.sparkContext,
      pgnPath,
      PGNExtractTransform.rankingUnUsedDataFilter)


    val rankingDF = joinedRankingRDDsToDF(sparkSession, gamesRDD, prItr)


    rankingDF
      .write
      .format("bigquery")
      .option("table", DATASET + ".ranking" + prItr.toString)
      .mode("overwrite").save()


  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("parameters missing")
      return
    }


    val pgnPath = args(0)
    val prItr = args(1).toInt
    val sparkSession = SparkSession
      .builder()
      .appName("lichess")
      .getOrCreate()
    rowRDDtoBigQuery(sparkSession, pgnPath, prItr)


  }


}
