import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object PGNExtractTransform {
  val DIRECTORY = "C:\\tmp_test\\"//todo
  val PGN_FILE: String = DIRECTORY + "lichess_db_standard_rated_2013-01.pgn.bz2"//todo



  def filterNull: String => Boolean = (x: String) => x != ""

  def filterEvent: String => Boolean = (x: String) => x.startsWith("[Event ")

  def filter_wight: String => Boolean = (x: String) => x.startsWith("[White ")

  def filter_black: String => Boolean = (x: String) => x.startsWith("[Black ")

  def filterResult: String => Boolean = (x: String) => x.startsWith("[Result ")

  def filterDate: String => Boolean = (x: String) => x.startsWith("[UTCDate ")

  def filterTime: String => Boolean = (x: String) => x.startsWith("[UTCTime ")


  def filterWRating: String => Boolean = (x: String) => x.startsWith("[WhiteElo ")

  def filterBRating: String => Boolean = (x: String) => x.startsWith("[BlackElo ")

  def filterWRatingLos: String => Boolean = (x: String) => x.startsWith("[WhiteRatingDiff ")

  def filterBRatingLos: String => Boolean = (x: String) => x.startsWith("[BlackRatingDiff ")

  def filterECO: String => Boolean = (x: String) => x.startsWith("[ECO ")

  def filterOpening: String => Boolean = (x: String) => x.startsWith("[Opening ")

  def filterTimeControl: String => Boolean = (x: String) => x.startsWith("[TimeControl ")

  def filterTermination: String => Boolean = (x: String) => x.startsWith("[Termination ")

  def filterPlays: String => Boolean = (x: String) => x.startsWith("1. ")


  def transformMapFun[T1, T2](mapper: (T1) => (T2)): ((T1, Long)) => ((T2, Long)) = {


    def transformFun = (s: (T1, Long)) => (mapper(s._1), s._2)


    transformFun

  }

  def transformFilterFun(filterFun: (String) => (Boolean)): ((Long, String)) => (Boolean) = {


    val transformFun = (s: (Long, String)) => filterFun(s._2)


    transformFun

  }



  def mapNames: String => String = (x: String) => x.substring(8).dropRight(2)

  def mapEvents: String => String = (x: String) => x.substring(14).dropRight(2)

  def mapEventsFarther(x: String): String = {
    //    println(res._1)
    if (x.startsWith("Bullet"))
      "Bullet"
    else if (x.startsWith("Blitz"))
      "Blitz"
    else if (x.startsWith("Classical"))
      "Classical"
    else
      "unknown event"
  }


  def mapResult: String => String = (x: String) => x.substring(9).dropRight(2)

  def mapDate: String => String = (x: String) => x.substring(10).dropRight(2)

  def mapTime: String => String = (x: String) => x.substring(10).dropRight(2)

  def mapWRating: String => String = (x: String) => x.substring(11).dropRight(2)

  def mapBRating: String => String = (x: String) => x.substring(11).dropRight(2)

  def mapWRatingLos: String => String = (x: String) => x.substring(16).dropRight(2)

  def mapBRatingLos: String => String = (x: String) => x.substring(16).dropRight(2)

  def mapECO: String => String = (x: String) => x.substring(6).dropRight(2)

  def mapOpening: String => String = (x: String) => x.substring(10).dropRight(2)

  def mapTimeControl: String => String = (x: String) => x.substring(14).dropRight(2)

  def mapTermination: String => String = (x: String) => x.substring(14).dropRight(2)



  def swapValKey[T1, T2](res: (T1, T2)): (T2, T1) = {

    (res._2, res._1)
  }

  def flat[T1, T2, T3](res: (T1, T2)): (T2, T1) = {

    (res._2, res._1)
  }

  def mapResult2(res: (String, Long)): (String, Long) = {
    if (res._1 == "1-0") {

      ("W", res._2)

    }


    else if (res._1 == "0-1") {

      ("B", res._2)
    }
    else {
      ("D", res._2)
    }
  }



  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[9]").setAppName("lichess")
    val sc = new SparkContext(conf)
    val games = pgnETLtoRowRDD(sc)
    games.foreach(println)


  }

  def pgnETLtoRowRDD(sc: SparkContext): RDD[Row] = {


//    val conf = new SparkConf().setMaster("local[9]").setAppName("lichess")
//    val sc = new SparkContext(conf)
    val pgn_file = sc.textFile(PGN_FILE).filter(filterNull)

    val events = pgn_file.filter(filterEvent).zipWithIndex().map(transformMapFun(mapEvents))
      .map(transformMapFun(mapEventsFarther))


    val wPlayers = pgn_file.filter(filter_wight).zipWithIndex().map(transformMapFun(mapNames))
    val bPlayers = pgn_file.filter(filter_black).zipWithIndex().map(transformMapFun(mapNames))
    //    println("Players: ", wPlayers.count(), bPlayers.count())

    val result = pgn_file.filter(filterResult).zipWithIndex().map(transformMapFun(mapResult)).map(mapResult2)
    //    println("result:", result.count())


    val date = pgn_file.filter(filterDate).zipWithIndex().map(transformMapFun(mapDate))
    //    println("date:", date.count())

    val time = pgn_file.filter(filterTime).zipWithIndex().map(transformMapFun(mapTime))
    //    println("time:", time.count())

    val wRating = pgn_file.filter(filterWRating).zipWithIndex().map(transformMapFun(mapWRating))
    val bRating = pgn_file.filter(filterBRating).zipWithIndex().map(transformMapFun(mapBRating))
    //    println("Rating: ", wRating.count(), bRating.count())


    val eco = pgn_file.filter(filterECO).zipWithIndex().map(transformMapFun(mapECO))
    //    println("eco:", eco.count())


    val Opening = pgn_file.filter(filterOpening).zipWithIndex().map(transformMapFun(mapOpening))
    //    println("Opening:", Opening.count())

    val timeControl = pgn_file.filter(filterTimeControl).zipWithIndex().map(transformMapFun(mapTimeControl))
    //    println("timeControl:", timeControl.count())

    val Termination = pgn_file.filter(filterTermination).zipWithIndex().map(transformMapFun(mapTermination))


    val plays = pgn_file.filter(filterPlays).zipWithIndex()
    //    println("Termination:", Termination.count())


    val gamesRDD = events.map(swapValKey)
      .join(wPlayers.map(swapValKey))
      .join(bPlayers.map(swapValKey))
      .join(result.map(swapValKey))
      .join(date.map(swapValKey))
      .join(time.map(swapValKey))
      .join(wRating.map(swapValKey))
      .join(bRating.map(swapValKey))
      .join(eco.map(swapValKey))
      .join(Opening.map(swapValKey))
      .join(timeControl.map(swapValKey))
      .join(Termination.map(swapValKey))
      .join(plays.map(swapValKey))


    gamesRDD.map(row)


  }

  def row(f: (Long, ((((((((((((String, String), String), String), String), String), String), String), String), String), String), String), String))): Row = {
    val (gameId, ((((((((((((event, wPlayerName), bPlayerName), winner), date), time), wRating), bRating), eco), opening), timeCtrl), termination), gamePlay)) = f

    val gameRow = sql.Row(gameId, event, wPlayerName, bPlayerName, winner, wRating, bRating, eco, opening, timeCtrl, date, time, termination, gamePlay)
    gameRow
  }


}
