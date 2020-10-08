import GamesLoader.UUIDLong
import Property._
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object PlayersRankingLoader {


  private def toRow(f: (VertexId, (Row, Option[Double]))): (VertexId, Row) = {

    (f._1, Row.merge(f._2._1, Row(f._2._2.getOrElse(0.0))))

  }

  private def toRow2(f: (VertexId, (Row, Option[Int]))): (VertexId, Row) = {

    (f._1, Row.merge(f._2._1, Row(f._2._2.getOrElse(0))))

  }


  private def toRatio(f: (VertexId, (Int, Int))): (VertexId, Double) = {
    (f._1, f._2._1.toDouble / f._2._2.toDouble)
  }

  private def toRatio2(f: (VertexId, (Double, Double))): (VertexId, Double) = {
    (f._1, f._2._1 / f._2._2)
  }

  private def toVal(f: (VertexId, (Option[Int], Option[Int]))): (VertexId, (Int, Int)) = {
    (f._1, (f._2._1.getOrElse(0), f._2._2.getOrElse(1)))
  }

  private def toVal2(f: (VertexId, (Option[Double], Option[Double]))): (VertexId, (Double, Double)) = {
    (f._1, (f._2._1.getOrElse(0), f._2._2.getOrElse(0)))
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
    val sc = new SparkContext(conf)


    playersRanRowRDDtoCSV(sc, Property.PGN_FILE)


  }

  private def mapToEdge(t: GameTupleFormat):
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


  private def wMapTupToVertex(t: GameTupleFormat): (VertexId, String) = {
    val (_, _, wPlayerName, _,
    _, _, _, _, _, _,
    _, _, _) = t

    val wId = UUIDLong(wPlayerName)


    (wId, wPlayerName)


  }

  private def bMapTupToVertex(t: GameTupleFormat): (VertexId, String) = {
    val (_, _, _, bPlayerName,
    _, _, _, _, _, _,
    _, _, _) = t

    val bId = UUIDLong(bPlayerName)


    (bId, bPlayerName)


  }


  private def wPlayerRatingAndName(f: EdgeTripletFormat): PlayerRatingAndNameFormat = {
    var rating = 0
    if (f.attr._5 != "?") {
      rating = f.attr._5.toInt
    }


    if (f.attr._4 == WHITE) {
      assert(f.srcAttr != null)
      ((f.srcId, f.srcAttr), rating)
    } else {
      assert(f.dstAttr != null)
      ((f.dstId, f.dstAttr), rating)
    }

  }

  private def bPlayerRatingAndName(f: EdgeTripletFormat): PlayerRatingAndNameFormat = {
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


  private def getVertexes(games: RDD[GameTupleFormat]): RDD[(VertexId, String)] = {
    games.map(wMapTupToVertex).distinct().union(games.map(bMapTupToVertex).distinct()).distinct()

  }

  private def getEdges(games: RDD[GameTupleFormat]) = {
    games.map(mapToEdge).filter(f => f.attr._4 != DRAW)
  }


  private def getPageRang(graph: GraphFormat, prItr: Int): VertexRDD[Double] = {

    graph.staticPageRank(prItr).vertices

  }


  private def getGamesPlayedRecords(graph: GraphFormat): (VertexRDD[Int], VertexRDD[Int], VertexRDD[Int]) = {

    val numOfGames = graph.degrees
    val numOfWins = graph.outDegrees
    val numOfLoses = graph.inDegrees
    (numOfGames, numOfWins, numOfLoses)
  }

  private def getPlayersAvgRating(graph: GraphFormat, vertexes: RDD[(VertexId, String)]): RDD[(VertexId, Int)] = {
    val wPlayer = graph.triplets.map(wPlayerRatingAndName).distinct()
    val bPlayer = graph.triplets.map(bPlayerRatingAndName).distinct()
    val wPlayerRating = wPlayer.map(f => (f._1._1, f._2))
    val bPlayerRating = bPlayer.map(f => (f._1._1, f._2))

    val playerName = vertexes

    val PlayerRating = bPlayerRating.union(wPlayerRating).groupByKey()
      .aggregateByKey(0)((a, b) => a + b.sum / b.size, (a, b) => a + b).distinct()
    PlayerRating
  }

  private def getWinLosRatio(numOfWins: VertexRDD[Int], numOfLoses: VertexRDD[Int]): RDD[(VertexId, Double)] = {
    numOfWins.fullOuterJoin(numOfLoses).map(toVal).map(toRatio)
  }

  private def getPRRatio(outerPRgraph: VertexRDD[Double], innerPRgraph: VertexRDD[Double]): RDD[(VertexId, Double)] = {
    outerPRgraph.fullOuterJoin(innerPRgraph).map(toVal2).map(toRatio2)

  }

  def tupleRDDtoPlayersRankingRdd(sc: SparkContext, pgnPath: String = Property.PGN_FILE, prItr: Int = 10): RDD[Row] = {
    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)


    val edges = getEdges(games)
    val vertexes = getVertexes(games) //games.map(wMapTupToVertex).distinct().union(games.map(bMapTupToVertex).distinct()).distinct()

    val graph = Graph(vertexes, edges)
    val graphClass = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("C")))
    val graphBullet = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Bullet")))
    val graphBlitz = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Blitz")))

    val innerPRgraph = getPageRang(graph, prItr) // graph.staticPageRank(prItr).vertices //.foreach(println)
    val outerPRgraph = getPageRang(graph.reverse, prItr) //graph.reverse.staticPageRank(prItr).vertices //    .foreach(println)
    val innerPRgraphClass = getPageRang(graphClass, prItr) //graphClass.staticPageRank(prItr).vertices //   .foreach(println)
    val outerPRgraphClass = getPageRang(graphClass.reverse, prItr) //graphClass.reverse.staticPageRank(prItr).vertices //    .foreach(println)
    val innerPRgraphBullet = getPageRang(graphBullet, prItr) //graphBullet.staticPageRank(prItr).vertices //.foreach(println)
    val outerPRgraphBullet = getPageRang(graphBullet.reverse, prItr) //graphBullet.reverse.staticPageRank(prItr).vertices //   .foreach(println)
    val innerPRgraphBlitz = getPageRang(graphBlitz, prItr) //graphBlitz.staticPageRank(prItr).vertices //.foreach(println)
    val outerPRgraphBlitz = getPageRang(graphBlitz.reverse, prItr) //graphBlitz.reverse.staticPageRank(prItr).vertices //   .foreach(println)


    //    val numOfGames = graph.degrees
    //    val numOfWins = graph.outDegrees
    //    val numOfLoses = graph.inDegrees
    val (numOfGames, numOfWins, numOfLoses) = getGamesPlayedRecords(graph)

    //    val numOfGamesClass = graphClass.degrees
    //    val numOfWinsClass = graphClass.outDegrees
    //    val numOfLosesClass = graphClass.inDegrees
    val (numOfGamesClass, numOfWinsClass, numOfLosesClass) = getGamesPlayedRecords(graph)

    //    val numOfGamesBullet = graphBullet.degrees
    //    val numOfWinsBullet = graphBullet.outDegrees
    //    val numOfLosesBullet = graphBullet.inDegrees
    val (numOfGamesBullet, numOfWinsBullet, numOfLosesBullet) = getGamesPlayedRecords(graph)

    //    val numOfGamesBlitz = graphBlitz.degrees
    //    val numOfWinsBlitz = graphBlitz.outDegrees
    //    val numOfLosesBlitz = graphBlitz.inDegrees
    val (numOfGamesBlitz, numOfWinsBlitz, numOfLosesBlitz) = getGamesPlayedRecords(graph)
    //
    //    val wPlayer = graph.triplets.map(wPlayerRatingAndName).distinct()
    //    val bPlayer = graph.triplets.map(bPlayerRatingAndName).distinct()
    //    val wPlayerRating = wPlayer.map(f => (f._1._1, f._2))
    //    val bPlayerRating = bPlayer.map(f => (f._1._1, f._2))
    //
    //    val playerName = vertexes
    //
    //    val PlayerRating = bPlayerRating.union(wPlayerRating).groupByKey()
    //      .aggregateByKey(0)((a, b) => a + b.sum / b.size, (a, b) => a + b).distinct()
    val playerName = vertexes
    val PlayerRating = getPlayersAvgRating(graph, vertexes)


    val winLosRatio = getWinLosRatio(numOfWins, numOfLoses) //numOfWins.fullOuterJoin(numOfLoses).map(toVal).map(toRatio)
    val winLosRatioClass = getWinLosRatio(numOfWinsClass, numOfLosesClass) // numOfWinsClass.fullOuterJoin(numOfLosesClass).map(toVal).map(toRatio)
    val winLosRatioBullet = getWinLosRatio(numOfWinsBullet, numOfLosesBullet) //numOfWinsBullet.fullOuterJoin(numOfLosesBullet).map(toVal).map(toRatio)
    val winLosRatioBlitz = getWinLosRatio(numOfWinsBlitz, numOfLosesBlitz) // numOfWinsBlitz.fullOuterJoin(numOfLosesBlitz).map(toVal).map(toRatio)
    //    getWinLosRatio(numOfWins,numOfLoses)

    val PRRatio = getPRRatio(outerPRgraph, innerPRgraph) //outerPRgraph.fullOuterJoin(innerPRgraph).map(toVal2).map(toRatio2)
    val PRRatioClass = getPRRatio(outerPRgraphClass, innerPRgraphClass) // outerPRgraphClass.fullOuterJoin(innerPRgraphClass).map(toVal2).map(toRatio2)
    val PRRatioBullet = getPRRatio(outerPRgraphBullet, innerPRgraphBullet) //outerPRgraphBullet.fullOuterJoin(innerPRgraphBullet).map(toVal2).map(toRatio2)
    val PRRatioBlitz = getPRRatio(outerPRgraphBlitz, innerPRgraphBlitz) //outerPRgraphBlitz.fullOuterJoin(innerPRgraphBlitz).map(toVal2).map(toRatio2)
    //    getPRRatio(outerPRgraph,innerPRgraph)


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


  def playersRanRowRDDtoCSV(sc: SparkContext, pgnPath: String = Property.PGN_FILE): Unit = {


    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = tupleRDDtoPlayersRankingRdd(sc, pgnPath)


    val df = spark.createDataFrame(gameTup, StructType(rankinSchema))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }
}
