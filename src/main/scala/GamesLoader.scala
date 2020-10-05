
import java.util.UUID

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object GamesLoader {
  val DIRECTORY = "C:\\tmp_test\\"
  //todo replace
  val PGN_FILE: String = DIRECTORY + "lichess_db_standard_rated_2013-01.pgn.bz2" //todo replace
  val PGN_FILE2: String = DIRECTORY + "lichess_db_standard_rated_2014-07.pgn.bz2" //todo replace


  val csvPath: String = DIRECTORY + "temporary\\tmp.csv"
  //todo replace
  val schemaString: String = "Id Event WightName BlackName Winner WightRating BlackRating" +
    " ECO Opening time-control Date Time Termination GamePlay"
  val schema: Array[String] = schemaString.split(" ")
  val schemaSeq: Seq[StructField] = Seq("Id", "Event", "WightName", "BlackName", "Winner",
    "WightRating", "BlackRating", "ECO", "Opening", "time-control"
    , "Date", "Time", "Termination", "GamePlay").map(field => StructField(field, StringType, nullable = false))

  def UUIDLong: String => Long = s => UUID.nameUUIDFromBytes(s.getBytes()).getLeastSignificantBits.abs

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
      .set("spark.executor.memory", "4G")
      .set("spark.driver.memory", "4G")
    val sc = new SparkContext(conf)


    playersRowRDDtoCSV(sc, PGN_FILE2)



  }

  def mapToEdge(t: (String, String, String, String, String, String, String, String,
    String, String, String, String, String)) = {
    val (_, event, wPlayerName, bPlayerName,
    winner, wRating, bRating, eco, opening, timeCtrl,
    date, time, termination) = t

    val wId = UUIDLong(wPlayerName)
    val bId = UUIDLong(bPlayerName)

    val edgeProperty = (event, wPlayerName, bPlayerName,
      winner, wRating, bRating, eco, opening, timeCtrl,
      date, time, termination)

    if ("B" == winner)
      Edge(bId, wId, edgeProperty)

    else
      Edge(wId, bId, edgeProperty)


  }


  def wMapRowToVertex(t: (String, String, String, String, String, String, String, String,
    String, String, String, String, String)): (VertexId, String) = {
    val (_, _, wPlayerName, _,
    _, _, _, _, _, _,
    _, _, _) = t

    val wId = UUIDLong(wPlayerName)


    (wId, wPlayerName)


  }

  def bMapRowToVertex(t: (String, String, String, String, String, String, String, String,
    String, String, String, String, String)): (VertexId, String) = {
    val (_, _, _, bPlayerName,
    _, _, _, _, _, _,
    _, _, _) = t

    val bId = UUIDLong(bPlayerName)


    (bId, bPlayerName)


  }

  //  def playerRating[a,b](t:EdgeTriplet)={
  //
  //  }

  type PlayerRatingIn = EdgeTriplet[String, (String, String, String, String,
    String, String, String, String, String, String, String, String)]
  type PlayerRatingOut = ((VertexId, String), Int)

  def mapWPlayerRating(f: PlayerRatingIn): PlayerRatingOut = {
    var rating = 0
    if (f.attr._5 != "?") {
      rating = f.attr._5.toInt
    }


    if (f.attr._4 == "W") {
      assert(f.srcAttr != null)
      ((f.srcId, f.srcAttr), rating)
    } else {
      assert(f.dstAttr != null)
      ((f.dstId, f.dstAttr), rating)
    }

  }

  def mapBPlayerRating(f: PlayerRatingIn): PlayerRatingOut = {
    var rating = 0
    if (f.attr._6 != "?") {
      rating = f.attr._6.toInt
    }


    if (f.attr._4 == "B") {
      assert(f.srcAttr != null)
      ((f.srcId, f.srcAttr), rating)
    }
    else {
      assert(f.dstAttr != null)
      ((f.dstId, f.dstAttr), rating)
    }

  }


  def tupleRDDtoGraphx(sc: SparkContext, pgnPath: String = PGN_FILE, prItr: Int = 10): RDD[Row] = {
    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)


    val edges = games.map(mapToEdge).filter(f => f.attr._4 != "D")
    val vertexes = games.map(wMapRowToVertex).distinct().union(games.map(bMapRowToVertex).distinct()).distinct()
    //    vertexes.foreach(f => assert(f._2 != null)) //todo
    //    println(vertexes.count())
    //    assert(false)
    val graph = Graph(vertexes, edges)
    val graphClass = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("C")))
    val graphBullet = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Bullet")))
    val graphBlitz = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Blitz")))
    val innerPRgraph = graph.staticPageRank(prItr).vertices //.map()    //.foreach(println)
    val outerPRgraph = graph.reverse.staticPageRank(prItr).vertices //    .foreach(println)
    val innerPRgraphClass = graphClass.staticPageRank(prItr).vertices //   .foreach(println)
    val outerPRgraphClass = graphClass.reverse.staticPageRank(prItr).vertices //    .foreach(println)
    val innerPRgraphBullet = graphBullet.staticPageRank(prItr).vertices //.foreach(println)
    val outerPRgraphBullet = graphBullet.reverse.staticPageRank(prItr).vertices //   .foreach(println)
    val innerPRgraphBlitz = graphBlitz.staticPageRank(prItr).vertices //.foreach(println)
    val outerPRgraphBlitz = graphBlitz.reverse.staticPageRank(prItr).vertices //   .foreach(println)
    val numOfGames = graph.degrees
    val numOfWins = graph.outDegrees
    val numOfLoses = graph.inDegrees

    val numOfGamesClass = graphClass.degrees
    val numOfWinsClass = graphClass.outDegrees
    val numOfLosesClass = graphClass.inDegrees

    val numOfGamesBullet = graphBullet.degrees
    val numOfWinsBullet = graphBullet.outDegrees
    val numOfLosesBullet = graphBullet.inDegrees

    val numOfGamesBlitz = graphBlitz.degrees
    val numOfWinsBlitz = graphBlitz.outDegrees
    val numOfLosesBlitz = graphBlitz.inDegrees


    val wPlayer = graph.triplets.map(mapWPlayerRating).distinct()
    val bPlayer = graph.triplets.map(mapBPlayerRating).distinct()
    val wPlayerRating = wPlayer.map(f => (f._1._1, f._2))
    val bPlayerRating = bPlayer.map(f => (f._1._1, f._2))

    val playerName = vertexes

    val PlayerRating = bPlayerRating.union(wPlayerRating).groupByKey()
      .aggregateByKey(0)((a, b) => a + b.sum / b.size, (a, b) => a + b).distinct()


    def toRow(f: (VertexId, (Row, Option[Double]))) = {

      (f._1, Row.merge(f._2._1, Row(f._2._2.getOrElse(0.0))))

    }
    def toRow2(f: (VertexId, (Row, Option[Int]))) = {

      (f._1, Row.merge(f._2._1, Row(f._2._2.getOrElse(0))))

    }




    def toRatio(f: (VertexId, (Int, Int))) = {
      (f._1, f._2._1.toDouble / f._2._2.toDouble)
    }

    def toRatio2(f: (VertexId, (Double, Double))) = {
      (f._1, f._2._1 / f._2._2)
    }

    def toVal(f: (VertexId, (Option[Int], Option[Int]))): (VertexId, (Int, Int)) = {
      (f._1, (f._2._1.getOrElse(0), f._2._2.getOrElse(1)))
    }

    def toVal2(f: (VertexId, (Option[Double], Option[Double]))): (VertexId, (Double, Double)) = {
      (f._1, (f._2._1.getOrElse(0), f._2._2.getOrElse(1)))
    }

    val winLosRatio = numOfWins.fullOuterJoin(numOfLoses).map(toVal).map(toRatio)
    val winLosRatioClass = numOfWinsClass.fullOuterJoin(numOfLosesClass).map(toVal).map(toRatio)
    val winLosRatioBullet = numOfWinsBullet.fullOuterJoin(numOfLosesBullet).map(toVal).map(toRatio)
    val winLosRatioBlitz = numOfWinsBlitz.fullOuterJoin(numOfLosesBlitz).map(toVal).map(toRatio)


    val PRRatio = outerPRgraph.fullOuterJoin(innerPRgraph).map(toVal2).map(toRatio2)
    val PRRatioClass = outerPRgraphClass.fullOuterJoin(innerPRgraphClass).map(toVal2).map(toRatio2)
    val PRRatioBullet = outerPRgraphBullet.fullOuterJoin(innerPRgraphBullet).map(toVal2).map(toRatio2)
    val PRRatioBlitz = outerPRgraphBlitz.fullOuterJoin(innerPRgraphBlitz).map(toVal2).map(toRatio2)






    //    val winLosRatio=numOfWins.leftOuterJoin(numOfLoses).map(toRatio)


    val finalData = playerName.map(f => (f._1, Row(f._2)))
      .leftOuterJoin(PlayerRating).map(toRow2)


      .leftOuterJoin(numOfGames).map(toRow2)
      .leftOuterJoin(winLosRatio).map(toRow)
      .leftOuterJoin(PRRatio).map(toRow)

      .leftOuterJoin(numOfGamesClass).map(toRow2)
      .leftOuterJoin(winLosRatioClass).map(toRow)
      .leftOuterJoin(PRRatioClass).map(toRow)

      .leftOuterJoin(numOfGamesBullet).map(toRow2)
      .leftOuterJoin(winLosRatioBullet).map(toRow)
      .leftOuterJoin(PRRatioBullet).map(toRow)


      .leftOuterJoin(numOfGamesBlitz).map(toRow2)
      .leftOuterJoin(winLosRatioBlitz).map(toRow)
      .leftOuterJoin(PRRatioBlitz).map(toRow)



    finalData.map(f => f._2)
  }

  type pJoinFormat = (VertexId, (((((((((((((((((((((String, PartitionID), Double), Double),
    Double), Double), Double), Double), Double), Double), PartitionID), PartitionID), PartitionID),
    PartitionID), PartitionID), PartitionID), PartitionID), PartitionID), PartitionID), PartitionID),
    PartitionID), PartitionID))
  //  type pJoinFormat = (VertexId, (((((((((((((((((((((String, PartitionID), Double), Double), Option[Double]),
  //    Option[Double]), Option[Double]), Option[Double]), Option[Double]), Option[Double]), PartitionID), PartitionID),
  //    PartitionID), Option[PartitionID]), Option[PartitionID]), Option[PartitionID]), Option[PartitionID]),
  //    Option[PartitionID]), Option[PartitionID]), Option[PartitionID]), Option[PartitionID]), Option[PartitionID]))




  def rowRDDtoCSV(sc: SparkContext, pgnPath: String = PGN_FILE): Unit = {


    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = PGNExtractTransform.pgnETtoRowRDD(sc, pgnPath)

    val df = spark.createDataFrame(gameTup, StructType(schemaSeq))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(csvPath)


  }


  val mapper= Map("name" -> StringType,
    "rating" -> IntegerType, "WinLosRatio" -> DoubleType, "TotalPR" -> DoubleType,
    "numOfGames" -> IntegerType, "numOfGamesClass" -> IntegerType,
    "numOfGamesBullet" -> IntegerType, "numOfGamesBlitz" -> IntegerType)
    .withDefault((a: String) => DoubleType)
  val schemaSeq2: Seq[StructField] = Seq("name", "rating",


    "numOfGames",
    "WinLosRatio",
    "TotalPRRatio",

    "numOfGamesClass",
    "winLosRatioClass",
    "PRRatioClass",

    "numOfGamesBullet",
    "winLosRatioBullet",
    "PRRatioBullet",

    "numOfGamesBlitz",
    "winLosRatioBlitz",
    "PRRatioBlitz",
  ).map(field => StructField(field, mapper(field), nullable = field!="name"))

  def playersRowRDDtoCSV(sc: SparkContext, pgnPath: String = PGN_FILE): Unit = {


    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = tupleRDDtoGraphx(sc, pgnPath)


    val df = spark.createDataFrame(gameTup, StructType(schemaSeq2))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(csvPath)


  }
}
