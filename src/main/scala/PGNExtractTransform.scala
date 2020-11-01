import Property._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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


  private def transformMapFun[T1, T2](mapper: T1 => T2): ((T1, Long)) => (T2, Long) = {


    def transformFun = (s: (T1, Long)) => (mapper(s._1), s._2)


    transformFun

  }


  private def mapNames: String => String = (x: String) => x.substring(8).dropRight(2)

  private def mapEvents: String => String = (x: String) => x.substring(14).dropRight(2)

  private def mapEventsFarther(x: String): String = {
    x.split(" ")(0)

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


  private def mapResult2(res: String): String = {
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
    rankingUnUsedDataFilter(data)||
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


    val pgn_file = sc.textFile(pgnPath)
      .filter(filterMethode)
      .cache()

    val events = pgn_file
      .filter(filterEvent)
      .map(mapEvents)
      .map(mapEventsFarther)


    val wPlayers = pgn_file
      .filter(filter_wight)
      .map(mapNames)
    val bPlayers = pgn_file
      .filter(filter_black)
      .map(mapNames)
    //    println("Players: ", wPlayers.count(), bPlayers.count())

    val result = pgn_file
      .filter(filterResult)
      .map(mapResult)
      .map(mapResult2)
    //    println("result:", result.count())

    val date = pgn_file
      .filter(filterDate)
      .map(mapDate)
    //    println("date:", date.count())

    val time = pgn_file
      .filter(filterTime)
      .map(mapTime)
    //    println("time:", time.count())

    val wRating = pgn_file
      .filter(filterWRating)
      .map(mapWRating)
    val bRating = pgn_file
      .filter(filterBRating)
      .map(mapBRating)
    //    println("Rating: ", wRating.count(), bRating.count())


    val eco = pgn_file
      .filter(filterECO)
      .map(mapECO)
    //    println("eco:", eco.count())


    val opening = pgn_file
      .filter(filterOpening)
      .map(mapOpening)
    //    println("Opening:", Opening.count())

    val timeControl = pgn_file
      .filter(filterTimeControl)
      .map(mapTimeControl)
    //    println("timeControl:", timeControl.count())

    val termination = pgn_file
      .filter(filterTermination)
      .map(mapTermination)
    //    println("Termination:", Termination.count())


    (
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
      ) = pgnETtoTupleRDDs(sc, pgnPath)

    val gamesRDD = events.zipWithIndex().map(swapValKey).filter(f => Property.filterValidator(f._2))
      .join(wPlayers.zipWithIndex().map(swapValKey))
      .join(bPlayers.zipWithIndex().map(swapValKey))
      .join(result.zipWithIndex().map(swapValKey))
      .join(date.zipWithIndex().map(swapValKey))
      .join(time.zipWithIndex().map(swapValKey))
      .join(wRating.zipWithIndex().map(swapValKey))
      .join(bRating.zipWithIndex().map(swapValKey))
      .join(eco.zipWithIndex().map(swapValKey))
      .join(opening.zipWithIndex().map(swapValKey))
      .join(timeControl.zipWithIndex().map(swapValKey))
      .join(termination.zipWithIndex().map(swapValKey))
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
