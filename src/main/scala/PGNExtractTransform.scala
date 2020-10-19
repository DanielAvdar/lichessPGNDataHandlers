import Property.{BLACK, DRAW, GameJoinFormat, GameTupleFormat, TupleRDDsFormat, WHITE}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object PGNExtractTransform {
  val DIRECTORY = "C:\\tmp_test\\"
  //todo replace
  val PGN_FILE: String = DIRECTORY + "lichess_db_standard_rated_2013-01.pgn.bz2" //todo replace


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
    if (x.startsWith("Bullet"))
      "Bullet"
    else if (x.startsWith("Blitz"))
      "Blitz"
    else if (x.startsWith("Classical"))
      "Classical"
    else
      "unknown event"
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


  private def swapValKey[T1, T2](res: (T1, T2)): (T2, T1) = {

    (res._2, res._1)
  }


  private def mapResult2(res: (String, Long)): (String, Long) = {
    if (res._1 == "1-0") {

      (WHITE, res._2)

    }


    else if (res._1 == "0-1") {

      (BLACK, res._2)
    }
    else {
      (DRAW, res._2)
    }
  }




  def pgnETtoTupleRDDs(sc: SparkContext, pgnPath: String = PGN_FILE):
  TupleRDDsFormat = {


    val pgn_file = sc.textFile(pgnPath).filter(filterNull)
      //      .repartition(1000)
      .cache()

    val events = pgn_file.filter(filterEvent).zipWithIndex().map(transformMapFun(mapEvents))//todo get rid of zipWithIndex
      .map(transformMapFun(mapEventsFarther))


    val wPlayers = pgn_file.filter(filter_wight).zipWithIndex().map(transformMapFun(mapNames))
    val bPlayers = pgn_file.filter(filter_black).zipWithIndex().map(transformMapFun(mapNames))
    //    println("Players: ", wPlayers.count(), bPlayers.count())

    val result = pgn_file.filter(filterResult).zipWithIndex().map(transformMapFun(mapResult)).map(mapResult2)
    //    println("result:", result.count())

    //    wPlayers.foreach(println)
    val date = pgn_file.filter(filterDate).zipWithIndex().map(transformMapFun(mapDate))
    //    println("date:", date.count())

    val time = pgn_file.filter(filterTime).zipWithIndex().map(transformMapFun(mapTime))
    //    println("time:", time.count())

    val wRating = pgn_file.filter(filterWRating).zipWithIndex().map(transformMapFun(mapWRating))
    val bRating = pgn_file.filter(filterBRating).zipWithIndex().map(transformMapFun(mapBRating))
    //    println("Rating: ", wRating.count(), bRating.count())


    val eco = pgn_file.filter(filterECO).zipWithIndex().map(transformMapFun(mapECO))
    //    println("eco:", eco.count())


    val opening = pgn_file.filter(filterOpening).zipWithIndex().map(transformMapFun(mapOpening))
    //    println("Opening:", Opening.count())

    val timeControl = pgn_file.filter(filterTimeControl).zipWithIndex().map(transformMapFun(mapTimeControl))
    //    println("timeControl:", timeControl.count())

    val termination = pgn_file.filter(filterTermination).zipWithIndex().map(transformMapFun(mapTermination))
    //    println("Termination:", Termination.count())


    pgn_file.unpersist()
    println("ETL start joins")


    (
      events.map(swapValKey),//todo map to up
      wPlayers.map(swapValKey),
      bPlayers.map(swapValKey),
      result.map(swapValKey),
      date.map(swapValKey),
      time.map(swapValKey),
      wRating.map(swapValKey),
      bRating.map(swapValKey),
      eco.map(swapValKey),
      opening.map(swapValKey),
      timeControl.map(swapValKey),
      termination.map(swapValKey)
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
      ) = pgnETtoTupleRDDs(sc,pgnPath)

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

  def pgnETtoRowRDD(sc: SparkContext, pgnPath: String = PGN_FILE): RDD[Row] = {


    val gamesRDD = TupleRDDsToJointTuple(sc, pgnPath)

    gamesRDD.map(tupleToRowFormat)


  }


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


}
