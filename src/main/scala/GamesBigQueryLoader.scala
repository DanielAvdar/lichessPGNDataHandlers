
import java.util.UUID

import PGNExtractTransform.rowRDDtoDataframe
import Property.{BUCKET, DATASET}

import org.apache.spark.sql.SparkSession

object GamesBigQueryLoader {

  def UUIDLong: String => Long = s => UUID.nameUUIDFromBytes(s.getBytes()).getLeastSignificantBits


  def rowRDDtoBigQuery(sparkSession: SparkSession, pgnPath: String): Unit = {


    val df = rowRDDtoDataframe(sparkSession, pgnPath)

    val bucket = BUCKET
    sparkSession.conf.set("temporaryGcsBucket", bucket)
    df.write.format("bigquery").option("table", DATASET + ".games").mode("overwrite").save()


  }


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("lichessGames")

      .getOrCreate()

    val pgnPath = args(0)

    rowRDDtoBigQuery(sparkSession, pgnPath)


  }
}
