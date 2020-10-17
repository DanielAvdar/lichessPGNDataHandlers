import GamesLoader.UUIDLong
import Property._
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
import org.apache.spark.{SparkConf, SparkContext}
//import org.neo4j.spark.Neo4jGraph
//import org.neo4j.spark.cypher.{NameProp, Pattern}


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

  //  def tupleRDDtoNeo4j(sc: SparkContext, pgnPath: String = PGN_FILE): Unit = {
  //    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)
  //
  //
  //    val edges = getEdges(games).map(f => Edge(f.srcId, f.dstId, f.attr._1))
  //    val vertexes = getVertexes(games)
  //
  //    val graph = Graph(vertexes, edges)
  //
  //    val innerPRgraph = graph.staticPageRank(10)
  //    // graph.staticPageRank(prItr).vertices //.foreach(println)
  //    val outerPRgraph = graph.reverse.staticPageRank(10)
  //    val totalPR = outerPRgraph.vertices.join(innerPRgraph.vertices)
  //      .map(f => (f._1, (f._2._1, f._2._2, f._2._1 / f._2._2)))
  //
  //
  //    val graphInner = Graph(totalPR.map(f => (f._1, f._2._1)), edges)
  //    val graphOuter = Graph(totalPR.map(f => (f._1, f._2._2)), edges)
  //    val graphRatio = Graph(totalPR.map(f => (f._1, f._2._3)), edges)
  //
  //    val neo = Neo4jGraph
  //
  //
  //    val p = Pattern(NameProp("Player", "identity"), Array(NameProp("event", "text")), NameProp("Player", "identity"))
  //
  //    neo.saveGraph(sc, graph, "name", relTypeProp = ("event", "text"), mainLabelIdProp = Some(p.target.asTuple),
  //      merge = true)
  //
  //    neo.saveGraph(sc, graphInner, "innerPR", relTypeProp = ("event", "text"), mainLabelIdProp = Some(p.target.asTuple),
  //      merge = false)
  //
  //    neo.saveGraph(sc, graphOuter, "outerPR", relTypeProp = ("event", "text"), mainLabelIdProp = Some(p.target.asTuple),
  //      merge = false)
  //    neo.saveGraph(sc, graphRatio, "RatioPR", relTypeProp = ("event", "text"), mainLabelIdProp = Some(p.target.asTuple),
  //      merge = false)
  //
  //
  //
  //  }


  def main2(args: Array[String]): Unit = {


  }


    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
      val sc = new SparkContext(conf)
      playersRanRowRDDtoCSV(sc)

      //
      //    for(i<-args)
      //      println(i)


      //    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
      //    val sc = new SparkContext(conf)
      //
      //    tupleRDDtoNeo4j(sc,Property.PGN_FILE)
      ////    playersRanRowRDDtoCSV(sc, Property.PGN_FILE)


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

  def tupleRDDtoPlayersRankingRddTmp(sc: SparkContext, pgnPath: String = Property.PGN_FILE, prItr: Int = 10) = {
    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)

    tupleRDDtoPlayersRankingRdd(games, prItr)
  }


  //  def rankingData()

  def rankingData(graph: GraphFormat, prItr: Int): (VertexRDD[Int], RDD[(VertexId, Double)], RDD[(VertexId, Double)]) = {
    graph.cache
    val innerPRgraph = getPageRang(graph, prItr)
    val outerPRgraph = getPageRang(graph.reverse, prItr)
    val (numOfGames, numOfWins, numOfLoses) = getGamesPlayedRecords(graph)
    graph.unpersist()
    val winLosRatio = getWinLosRatio(numOfWins, numOfLoses)
    val gPRRatio = getPRRatio(outerPRgraph, innerPRgraph)

    (numOfGames, winLosRatio, gPRRatio)

  }

  def rankingDataToRow(graph: GraphFormat, prItr: Int): RDD[(VertexId, Row)] = {

    val (numOfGames, winLosRatio, gPRRatio) = rankingData(graph, prItr)
//    println(numOfGames.count(),winLosRatio.count(),gPRRatio.count())
//    assert(false)
//    numOfGames.foreach(println)
    val row = gPRRatio.map(f => (f._1, Row(f._2)))//.map(,toRow2)
      .leftOuterJoin(numOfGames).map(toRow2)
      .leftOuterJoin(winLosRatio).map(toRow)
//    println(row.count())

    row

  }

  def tupleRDDtoPlayersRankingRdd(games: RDD[GameTupleFormat], prItr: Int = 10): RDD[Row] = {
    //    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)

    println("tupleRDDtoPlayersRankingRdd")
    val edges = getEdges(games).persist(MEMORY_AND_DISK_SER)
    val vertexes = getVertexes(games).persist(MEMORY_AND_DISK_SER)

    val graph = Graph(vertexes, edges)
    val graphClass = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("C")))
    val graphBullet = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Bullet")))
    val graphBlitz = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Blitz")))
    val graphRapid = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Rapid")))
    println("Graphs ready")

    val playerName = vertexes
    val PlayerRating = getPlayersAvgRating(graph, vertexes)
    edges.unpersist()
    vertexes.unpersist()
    //    rankingData(graph)
    println("Start ranking")


    val allGames=rankingDataToRow(graph,prItr)
    val calassGames=rankingDataToRow(graphClass,prItr)
    val bulletGames=rankingDataToRow(graphBullet,prItr)
    val blitzGames=rankingDataToRow(graphBlitz,prItr)
    val rapidGames=rankingDataToRow(graphRapid,prItr)

    println("End ranking")





    val finalData = playerName.map(f => (f._1, Row(f._2)))
      .leftOuterJoin(PlayerRating).map(toRow2)
      .join(allGames).map(f=>(f._1,Row.merge(f._2._1,f._2._2)))
      .join(calassGames).map(f=>(f._1,Row.merge(f._2._1,f._2._2)))
      .join(bulletGames).map(f=>(f._1,Row.merge(f._2._1,f._2._2)))
      .join(blitzGames).map(f=>(f._1,Row.merge(f._2._1,f._2._2)))
      .join(rapidGames).map(f=>(f._1,Row.merge(f._2._1,f._2._2)))
      .map(f => f._2)

    println("End joins")


    finalData
  }

  def tupleRDDtoPlayersRankingRdd2(games: RDD[GameTupleFormat], prItr: Int = 10): RDD[Row] = {
    //    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)


    val edges = getEdges(games).persist(MEMORY_AND_DISK_SER)
    val vertexes = getVertexes(games).persist(MEMORY_AND_DISK_SER)

    val graph = Graph(vertexes, edges).cache
    val graphClass = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("C")))
    val graphBullet = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Bullet")))
    val graphBlitz = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Blitz")))
    val graphRapid = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Rapid")))

    val playerName = vertexes
    val PlayerRating = getPlayersAvgRating(graph, vertexes)
    edges.unpersist()
    vertexes.unpersist()
    //    rankingData(graph)

    val innerPRgraph = getPageRang(graph, prItr)
    val outerPRgraph = getPageRang(graph.reverse, prItr)

    val innerPRgraphClass = getPageRang(graphClass, prItr)
    val outerPRgraphClass = getPageRang(graphClass.reverse, prItr)

    val innerPRgraphBullet = getPageRang(graphBullet, prItr)
    val outerPRgraphBullet = getPageRang(graphBullet.reverse, prItr)

    val innerPRgraphBlitz = getPageRang(graphBlitz, prItr)
    val outerPRgraphBlitz = getPageRang(graphBlitz.reverse, prItr)

    val innerPRgraphRapid = getPageRang(graphRapid, prItr)
    val outerPRgraphRapid = getPageRang(graphRapid.reverse, prItr)


    val (numOfGames, numOfWins, numOfLoses) = getGamesPlayedRecords(graph)


    val (numOfGamesClass, numOfWinsClass, numOfLosesClass) = getGamesPlayedRecords(graphClass)


    val (numOfGamesBullet, numOfWinsBullet, numOfLosesBullet) = getGamesPlayedRecords(graphBullet)


    val (numOfGamesBlitz, numOfWinsBlitz, numOfLosesBlitz) = getGamesPlayedRecords(graphBlitz)


    val (numOfGamesRapid, numOfWinsRapid, numOfLosesRapid) = getGamesPlayedRecords(graphRapid)


    val winLosRatio = getWinLosRatio(numOfWins, numOfLoses)
    val winLosRatioClass = getWinLosRatio(numOfWinsClass, numOfLosesClass)
    val winLosRatioBullet = getWinLosRatio(numOfWinsBullet, numOfLosesBullet)
    val winLosRatioBlitz = getWinLosRatio(numOfWinsBlitz, numOfLosesBlitz)
    val winLosRatioRapid = getWinLosRatio(numOfWinsRapid, numOfLosesRapid)


    val PRRatio = getPRRatio(outerPRgraph, innerPRgraph)
    val PRRatioClass = getPRRatio(outerPRgraphClass, innerPRgraphClass)
    val PRRatioBullet = getPRRatio(outerPRgraphBullet, innerPRgraphBullet)
    val PRRatioBlitz = getPRRatio(outerPRgraphBlitz, innerPRgraphBlitz)
    val PRRatioRapid = getPRRatio(outerPRgraphRapid, innerPRgraphRapid)

    //    val (numOfGames,winLosRatio,PRRatio)=rankingData(graph,prItr)


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


      .leftOuterJoin(numOfGamesRapid).map(toRow2)
      .leftOuterJoin(winLosRatioRapid).map(toRow)
      .leftOuterJoin(PRRatioRapid).map(toRow)
      .map(f => f._2)


    finalData
  }


  def playersRanRowRDDtoCSV(sc: SparkContext, pgnPath: String = Property.PGN_FILE): Unit = {

    sc.setLogLevel("ERROR")

    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = tupleRDDtoPlayersRankingRddTmp(sc, pgnPath)


    val df = spark.createDataFrame(gameTup, StructType(rankinSchema))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }
}
