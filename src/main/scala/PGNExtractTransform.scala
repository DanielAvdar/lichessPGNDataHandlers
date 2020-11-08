import Property._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER

object PGNExtractTransform {


  private def filterNull: String => Boolean = (x: String) => x != ""

  private def filterEvent: String => Boolean = (x: String) => x.startsWith("[Event ")

  private def filter_wight: String => Boolean = (x: String) => x.startsWith("[White ")

  private def filter_black: String => Boolean = (x: String) => x.startsWith("[Black ")

  private def filterResult: String => Boolean = (x: String) => x.startsWith("[Result ")

  private def filterDate: String => Boolean = (x: String) => x.startsWith("[UTCDate ")

  private def filterTime: String => Boolean = (x: String) => x.startsWith("[UTCTime ")


  private def filterWRating: String => Boolean = (x: String) => x.startsWith("[WhiteElo ")

  private def filterBRating: String => Boolean = (x: String) => x.startsWith("[BlackElo ")


  private def filterECO: String => Boolean = (x: String) => x.startsWith("[ECO ")

  private def filterOpening: String => Boolean = (x: String) => x.startsWith("[Opening ")

  private def filterTimeControl: String => Boolean = (x: String) => x.startsWith("[TimeControl ")

  private def filterTermination: String => Boolean = (x: String) => x.startsWith("[Termination ")


  private def mapNames: String => String = (x: String) => x.substring(8).dropRight(2)

  private def mapEvents: String => String = (x: String) => x.substring(14).dropRight(2)

  private def mapAndCleanEvents(x: String): String = {
    val tmp = mapEvents(x)
    tmp.split(" ")(0)

  }


  private def mapResult: String => String = (x: String) => x.substring(9).dropRight(2)

  private def mapDate: String => String = (x: String) => x.substring(10).dropRight(2)

  private def mapTime: String => String = (x: String) => x.substring(10).dropRight(2)

  private def mapWRating: String => String = (x: String) => x.substring(11).dropRight(2)

  private def mapBRating: String => String = (x: String) => x.substring(11).dropRight(2)


  private def mapECO: String => String = (x: String) => x.substring(6).dropRight(2)

  private def mapOpening: String => String = (x: String) => x.substring(10).dropRight(2)

  private def mapTimeControl: String => String = (x: String) => x.substring(14).dropRight(2)

  private def mapTermination: String => String = (x: String) => x.substring(14).dropRight(2)


  def swapValKey[T1, T2](res: (T1, T2)): (T2, T1) = {

    (res._2, res._1)
  }


  private def mapResult2(res2: String): String = {
    val res = mapResult(res2)
    if (res == "1-0") {

      WHITE

    }


    else if (res == "0-1") {

      BLACK
    }
    else {
      DRAW
    }
  }

  private def unUsedDataFilter(data: String): Boolean = {
    rankingUnUsedDataFilter(data) ||
      filterDate(data) ||
      filterTime(data) ||
      filterWRating(data) ||
      filterBRating(data) ||
      filterECO(data) ||
      filterOpening(data) ||
      filterTimeControl(data) ||
      filterTermination(data)

  }

  def rankingUnUsedDataFilter(data: String): Boolean = {
    filterNull(data) ||
      filterEvent(data) ||
      filter_wight(data) ||
      filter_black(data) ||
      filterResult(data)


  }

  def pgnETtoTupleRDDs(sc: SparkContext, pgnPath: String, filterMethode: String => Boolean = unUsedDataFilter): TupleRDDsFormat = {


    val pgnFile = sc.textFile(pgnPath, 150)
      .filter(filterMethode)
      .persist(MEMORY_AND_DISK_SER)
    val events = pgnFile
      .map(mapAndCleanEvents)
      .filter(filterEvent)


    val wPlayers = pgnFile
      .filter(filter_wight)
      .map(mapNames)
    val bPlayers = pgnFile
      .filter(filter_black)
      .map(mapNames)

    val result = pgnFile
      .filter(filterResult)
      .map(mapResult2)

    val date = pgnFile
      .filter(filterDate)
      .map(mapDate)

    val time = pgnFile
      .filter(filterTime)
      .map(mapTime)

    val wRating = pgnFile
      .filter(filterWRating)
      .map(mapWRating)
    val bRating = pgnFile
      .filter(filterBRating)
      .map(mapBRating)


    val eco = pgnFile
      .filter(filterECO)
      .map(mapECO)


    val opening = pgnFile
      .filter(filterOpening)
      .map(mapOpening)

    val timeControl = pgnFile
      .filter(filterTimeControl)
      .map(mapTimeControl)

    val termination = pgnFile
      .filter(filterTermination)
      .map(mapTermination)


    val res = (
      pgnFile,
      events,
      wPlayers,
      bPlayers,
      result,
      date,
      time,
      wRating,
      bRating,
      eco,
      opening,
      timeControl,
      termination
    )
    res

  }

  def zipRDDsWithIndex(sc: SparkContext, pgnPath: String):
  TupleRDDsWithIndexFormat = {
    val (
      pgnFile,
      events,
      wPlayers,
      bPlayers,
      result,
      date,
      time,
      wRating,
      bRating,
      eco,
      opening,
      timeControl,
      termination
      ) = pgnETtoTupleRDDs(sc, pgnPath)
    val res = (
      events.zipWithIndex().map(swapValKey).filter(f => Property.filterValidator(f._2)),
      wPlayers.zipWithIndex().map(swapValKey),
      bPlayers.zipWithIndex().map(swapValKey),
      result.zipWithIndex().map(swapValKey),
      date.zipWithIndex().map(swapValKey),
      time.zipWithIndex().map(swapValKey),
      wRating.zipWithIndex().map(swapValKey),
      bRating.zipWithIndex().map(swapValKey),
      eco.zipWithIndex().map(swapValKey),
      opening.zipWithIndex().map(swapValKey),
      timeControl.zipWithIndex().map(swapValKey),
      termination.zipWithIndex().map(swapValKey)
    )
    pgnFile.unpersist()
    res
  }

  def TupleRDDsToJointTuple(sc: SparkContext, pgnPath: String):
  RDD[GameTupleFormat] = {
    val (
      events,
      wPlayers,
      bPlayers,
      result,
      date,
      time,
      wRating,
      bRating,
      eco,
      opening,
      timeControl,
      termination
      ) = zipRDDsWithIndex(sc, pgnPath)

    val gamesRDD = events
      .join(wPlayers)
      .join(bPlayers)
      .join(result)
      .join(date)
      .join(time)
      .join(wRating)
      .join(bRating)
      .join(eco)
      .join(opening)
      .join(timeControl)
      .join(termination)

    gamesRDD.map(toFlatTuple)


  }

  def pgnETtoRowRDD(sc: SparkContext, pgnPath: String): RDD[Row] = {


    val gamesRDD = TupleRDDsToJointTuple(sc, pgnPath)

    gamesRDD.map(tupleToRowFormat)


  }

  def pgnETtoDataFrame(spark: SparkSession, pgnPath: String): DataFrame = {

    val gamesRowRDD = pgnETtoRowRDD(spark.sparkContext, pgnPath)
    spark.createDataFrame(gamesRowRDD, StructType(Property.gameSchema))

  }

  def rowRDDtoDataframe(sparkSession: SparkSession, pgnPath: String = Property.PGN_FILE): DataFrame = {


    val gameTup = PGNExtractTransform.pgnETtoRowRDD(sparkSession.sparkContext, pgnPath)

    val df = sparkSession.createDataFrame(gameTup, StructType(Property.gameSchema))


    df


  }


  //test
  def tupleToRowFormat(f: GameTupleFormat): Row = {
    val (_, event, wPlayerName, bPlayerName, winner, wRating, bRating, eco,
    opening, timeCtrl, date, time, termination) = f
    val f2 = (event, wPlayerName, bPlayerName, winner, wRating, bRating, eco,
      opening, timeCtrl, date, time, termination)
    val gameRow = sql.Row.fromTuple(f2)
    gameRow
  }

  private def toFlatTuple(f: GameJoinFormat): GameTupleFormat = {
    val (gameId, (((((((((((event, wPlayerName), bPlayerName), winner),
    date), time), wRating), bRating), eco), opening), timeCtrl), termination)) = f

    (gameId.toString, event, wPlayerName, bPlayerName, winner, wRating, bRating, eco,
      opening, timeCtrl, date, time, termination)

  }

  def rowRDDtoCSV(sparkSession: SparkSession, pgnPath: String = Property.PGN_FILE): Unit = {


    val df = rowRDDtoDataframe(sparkSession, pgnPath)


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }

  //test
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local[12]")
      .appName("lichess")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")

    rowRDDtoCSV(sparkSession)

  }
}
