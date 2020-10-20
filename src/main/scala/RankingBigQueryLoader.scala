import PlayersRanking.joinedRankingRDDsToDF
import Property.{BUCKET, DATASET}
import org.apache.spark.sql.SparkSession

object RankingBigQueryLoader {

  def rowRDDtoBigQuery(sparkSession: SparkSession, pgnPath: String = Property.PGN_FILE): Unit = {


    val bucket = BUCKET
    sparkSession.conf.set("temporaryGcsBucket", bucket)


    val gamesRDD = PGNExtractTransform.pgnETtoTupleRDDs(sparkSession.sparkContext, pgnPath)


    val rankingDF = joinedRankingRDDsToDF(sparkSession, gamesRDD)


    rankingDF.write.format("bigquery").option("table", DATASET + ".ranking").mode("overwrite").save()


  }

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("parameters missing")
      return
    }


    val pgnPath = args(0)
    val sparkSession = SparkSession
      .builder()
      .appName("lichess")
      .getOrCreate()
    rowRDDtoBigQuery(sparkSession, pgnPath)






    //https://stackoverflow.com/questions/31728688/how-to-prevent-spark-executors-from-getting-lost-when-using-yarn-client-mode//todo


  }


}
