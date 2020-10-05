import GamesLoader.UUIDLong
import Property._
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object PlayersRankingLoader {


  def toRow(f: (VertexId, (Row, Option[Double]))): (VertexId, Row) = {

    (f._1, Row.merge(f._2._1, Row(f._2._2.getOrElse(0.0))))

  }

  def toRow2(f: (VertexId, (Row, Option[Int]))): (VertexId, Row) = {

    (f._1, Row.merge(f._2._1, Row(f._2._2.getOrElse(0))))

  }


  def toRatio(f: (VertexId, (Int, Int))): (VertexId, Double) = {
    (f._1, f._2._1.toDouble / f._2._2.toDouble)
  }

  def toRatio2(f: (VertexId, (Double, Double))): (VertexId, Double) = {
    (f._1, f._2._1 / f._2._2)
  }

  def toVal(f: (VertexId, (Option[Int], Option[Int]))): (VertexId, (Int, Int)) = {
    (f._1, (f._2._1.getOrElse(0), f._2._2.getOrElse(1)))
  }

  def toVal2(f: (VertexId, (Option[Double], Option[Double]))): (VertexId, (Double, Double)) = {
    (f._1, (f._2._1.getOrElse(0), f._2._2.getOrElse(1)))
  }










  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
      .set("spark.executor.memory", "4G")
      .set("spark.driver.memory", "4G")
    val sc = new SparkContext(conf)


    playersRowRDDtoCSV(sc, Property.PGN_FILE)


  }

  def mapToEdge(t: GameTupleFormat):
  EdgeFormat = {
    val (_, event, wPlayerName, bPlayerName,
    winner, wRating, bRating, eco, opening, timeCtrl,
    date, time, termination) = t

    val wId = UUIDLong(wPlayerName)
    val bId = UUIDLong(bPlayerName)

    val edgeProperty = (event, wPlayerName, bPlayerName,
      winner, wRating, bRating, eco, opening, timeCtrl,
      date, time, termination)

    if (BLACK == winner)
      Edge(bId, wId, edgeProperty)

    else
      Edge(wId, bId, edgeProperty)


  }


  def wMapTupToVertex(t:GameTupleFormat): (VertexId, String) = {
    val (_, _, wPlayerName, _,
    _, _, _, _, _, _,
    _, _, _) = t

    val wId = UUIDLong(wPlayerName)


    (wId, wPlayerName)


  }

  def bMapTupToVertex(t: GameTupleFormat): (VertexId, String) = {
    val (_, _, _, bPlayerName,
    _, _, _, _, _, _,
    _, _, _) = t

    val bId = UUIDLong(bPlayerName)


    (bId, bPlayerName)


  }




  def wPlayerRatingAndName(f: EdgeTripletFormat): PlayerRatingAndNameFormat = {
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

  def bPlayerRatingAndName(f: EdgeTripletFormat): PlayerRatingAndNameFormat = {
    var rating = 0
    if (f.attr._6 != "?") {
      rating = f.attr._6.toInt
    }


    if (f.attr._4 == BLACK) {
      assert(f.srcAttr != null)
      ((f.srcId, f.srcAttr), rating)
    }
    else {
      assert(f.dstAttr != null)
      ((f.dstId, f.dstAttr), rating)
    }

  }


  def tupleRDDtoGraphx(sc: SparkContext, pgnPath: String = Property.PGN_FILE, prItr: Int = 10): RDD[Row] = {
    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)


    val edges = games.map(mapToEdge).filter(f => f.attr._4 != "D")
    val vertexes = games.map(wMapTupToVertex).distinct().union(games.map(bMapTupToVertex).distinct()).distinct()

    val graph = Graph(vertexes, edges)
    val graphClass = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("C")))
    val graphBullet = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Bullet")))
    val graphBlitz = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Blitz")))
    val innerPRgraph = graph.staticPageRank(prItr).vertices //.foreach(println)
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


    val wPlayer = graph.triplets.map(wPlayerRatingAndName).distinct()
    val bPlayer = graph.triplets.map(bPlayerRatingAndName).distinct()
    val wPlayerRating = wPlayer.map(f => (f._1._1, f._2))
    val bPlayerRating = bPlayer.map(f => (f._1._1, f._2))

    val playerName = vertexes

    val PlayerRating = bPlayerRating.union(wPlayerRating).groupByKey()
      .aggregateByKey(0)((a, b) => a + b.sum / b.size, (a, b) => a + b).distinct()



    val winLosRatio = numOfWins.fullOuterJoin(numOfLoses).map(toVal).map(toRatio)
    val winLosRatioClass = numOfWinsClass.fullOuterJoin(numOfLosesClass).map(toVal).map(toRatio)
    val winLosRatioBullet = numOfWinsBullet.fullOuterJoin(numOfLosesBullet).map(toVal).map(toRatio)
    val winLosRatioBlitz = numOfWinsBlitz.fullOuterJoin(numOfLosesBlitz).map(toVal).map(toRatio)


    val PRRatio = outerPRgraph.fullOuterJoin(innerPRgraph).map(toVal2).map(toRatio2)
    val PRRatioClass = outerPRgraphClass.fullOuterJoin(innerPRgraphClass).map(toVal2).map(toRatio2)
    val PRRatioBullet = outerPRgraphBullet.fullOuterJoin(innerPRgraphBullet).map(toVal2).map(toRatio2)
    val PRRatioBlitz = outerPRgraphBlitz.fullOuterJoin(innerPRgraphBlitz).map(toVal2).map(toRatio2)


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
      .leftOuterJoin(PRRatioBlitz).map(toRow).map(f => f._2)

    finalData
  }






  def playersRowRDDtoCSV(sc: SparkContext, pgnPath: String = Property.PGN_FILE): Unit = {


    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = tupleRDDtoGraphx(sc, pgnPath)


    val df = spark.createDataFrame(gameTup, StructType(rankinSchema))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }
}
