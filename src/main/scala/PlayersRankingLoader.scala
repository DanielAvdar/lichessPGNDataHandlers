import GamesLoader.UUIDLong
import PGNExtractTransform.pgnETtoTupleRDDs
import Property._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER
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

  def MapTupToVertex(name: String): (VertexId, String) = {

    val bId = UUIDLong(name)


    (bId, name)

  }

  private def getVertexes3(wPlayers: RDD[(Long, String)], bPlayers: RDD[(Long, String)]) = {
    wPlayers
      .map(f => f._2)
      .distinct()
      .union(bPlayers
        .map(f => f._2)
        .distinct())
      .distinct
      .map(MapTupToVertex)

  }

  private def getEdges(games: RDD[GameTupleFormat]) = {
    games.map(mapToEdge).filter(f => f.attr._4 != DRAW)
  }

  //  private def getEdges3(games: RDD[GameTupleFormat]) = {
  //    games.map(mapToEdge).filter(f => f.attr._4 != DRAW)
  //  }


  private def getPageRang(graph: GraphFormat2, prItr: Int): VertexRDD[Double] = {

    graph.staticPageRank(prItr).vertices

  }


  private def getGamesPlayedRecords(graph: GraphFormat2): (VertexRDD[Int], VertexRDD[Int], VertexRDD[Int]) = {

    val numOfGames = graph.degrees
    val numOfWins = graph.outDegrees
    val numOfLoses = graph.inDegrees
    (numOfGames, numOfWins, numOfLoses)
  }


  private def getWinLosRatio(numOfWins: VertexRDD[Int], numOfLoses: VertexRDD[Int]): RDD[(VertexId, Double)] = {
    numOfWins.fullOuterJoin(numOfLoses).map(toVal).map(toRatio)
  }

  private def getPRRatio(outerPRgraph: VertexRDD[Double], innerPRgraph: VertexRDD[Double]): RDD[(VertexId, Double)] = {
    outerPRgraph.fullOuterJoin(innerPRgraph).map(toVal2).map(toRatio2)

  }

  def tupleRDDtoPlayersRankingRddTmp(sc: SparkContext, pgnPath: String = Property.PGN_FILE, prItr: Int = 10) = {
    val games = PGNExtractTransform.pgnETtoTupleRDDs(sc, pgnPath)

    tupleRDDtoPlayersRankingRdd3(games, prItr)
  }


//  def rankingData(graph: GraphFormat, prItr: Int): (VertexRDD[Int], RDD[(VertexId, Double)], RDD[(VertexId, Double)]) = {
//    graph.cache
//    val innerPRgraph = getPageRang(graph, prItr)
//    val outerPRgraph = getPageRang(graph.reverse, prItr)
//    val (numOfGames, numOfWins, numOfLoses) = getGamesPlayedRecords(graph)
//    graph.unpersist()
//    val winLosRatio = getWinLosRatio(numOfWins, numOfLoses)
//    val gPRRatio = getPRRatio(outerPRgraph, innerPRgraph)
//
//    (numOfGames, winLosRatio, gPRRatio)
//
//  }

//  def rankingDataToRow(graph: GraphFormat, prItr: Int): RDD[(VertexId, Row)] = {
//
//    val (numOfGames, winLosRatio, gPRRatio) = rankingData(graph, prItr)
//
//    val row = gPRRatio.map(f => (f._1, Row(f._2)))
//      .leftOuterJoin(numOfGames).map(toRow2)
//      .leftOuterJoin(winLosRatio).map(toRow)
//
//    row
//
//  }

  //  def tupleRDDtoPlayersRankingRdd(games: RDD[GameTupleFormat], prItr: Int = 10): RDD[Row] = {
  //    //    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)
  //
  //    println("tupleRDDtoPlayersRankingRdd")
  //    val edges = getEdges(games).persist(MEMORY_AND_DISK_SER)
  //    val vertexes = getVertexes(games).persist(MEMORY_AND_DISK_SER)
  //
  //    val graph = Graph(vertexes, edges)
  //    val graphClass = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("C")))
  //    val graphBullet = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Bullet")))
  //    val graphBlitz = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Blitz")))
  //    val graphRapid = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Rapid")))
  //    println("Graphs ready")
  //
  //    val playerName = vertexes
  //    val PlayerRating = getPlayersAvgRating(graph, vertexes)
  //    edges.unpersist()
  //    vertexes.unpersist()
  //    //    rankingData(graph)
  //    println("Start ranking")
  //
  //
  //    val allGames = rankingDataToRow(graph, prItr)
  //    val calassGames = rankingDataToRow(graphClass, prItr)
  //    val bulletGames = rankingDataToRow(graphBullet, prItr)
  //    val blitzGames = rankingDataToRow(graphBlitz, prItr)
  //    val rapidGames = rankingDataToRow(graphRapid, prItr)
  //
  //    println("End ranking")
  //
  //
  //    val finalData = playerName.map(f => (f._1, Row(f._2)))
  //      .leftOuterJoin(PlayerRating).map(toRow2)
  //      .join(allGames).map(f => (f._1, Row.merge(f._2._1, f._2._2)))
  //      .join(calassGames).map(f => (f._1, Row.merge(f._2._1, f._2._2)))
  //      .join(bulletGames).map(f => (f._1, Row.merge(f._2._1, f._2._2)))
  //      .join(blitzGames).map(f => (f._1, Row.merge(f._2._1, f._2._2)))
  //      .join(rapidGames).map(f => (f._1, Row.merge(f._2._1, f._2._2)))
  //      .map(f => f._2)
  //
  //    println("End joins")
  //
  //
  //    finalData
  //  }
  private def mapToEdge3exmp(t: GameTupleFormat):
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


  def mapToEdge3(edgesProperty: GameTupleFormat2): Edge[GameTupleFormat2] = {

    val (_, _, winner, wPlayers, bPlayers) = edgesProperty

    val wId = UUIDLong(wPlayers)
    val bId = UUIDLong(bPlayers)

    if (BLACK == winner)
      Edge(bId, wId, edgesProperty)

    else
      Edge(wId, bId, edgesProperty)

  }


  def mapToProper(edgesProperty: (Long, (((String, String), String), String))): GameTupleFormat2 = {

    val (gameId, (((events, winner), wPlayers), bPlayers)) = edgesProperty

    (gameId, events, winner, wPlayers, bPlayers)

  }

  def getEdgesProperty(events: RDD[(VertexId, String)], result: RDD[(VertexId, String)],
                       wPlayers: RDD[(VertexId, String)], bPlayers: RDD[(VertexId, String)]): RDD[GameTupleFormat2] = {
    events.persist(MEMORY_AND_DISK_SER)
    result.persist(MEMORY_AND_DISK_SER)
    wPlayers.persist(MEMORY_AND_DISK_SER)
    bPlayers.persist(MEMORY_AND_DISK_SER)

    val edgesProperty = events
      .join(result)
      .join(wPlayers)
      .join(bPlayers)
      .map(mapToProper)
    edgesProperty
  }


  //  private def getPlayersAvgRating(graph: GraphFormat, vertexes: RDD[(VertexId, String)]): RDD[(VertexId, Int)] = {
  //    val wPlayer = graph.triplets.map(wPlayerRatingAndName).distinct()
  //    val bPlayer = graph.triplets.map(bPlayerRatingAndName).distinct()
  //    val wPlayerRating = wPlayer.map(f => (f._1._1, f._2))
  //    val bPlayerRating = bPlayer.map(f => (f._1._1, f._2))
  //
  //    val playerName = vertexes
  //
  //    val PlayerRating = bPlayerRating.union(wPlayerRating).groupByKey()
  //      .aggregateByKey(0)((a, b) => a + b.sum / b.size, (a, b) => a + b).distinct()
  //    PlayerRating
  //  }

  private def wPlayerRatingAndName(f: EdgeTriplet[String, GameTupleFormat2]) = {
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


  def filterGraphByEvent(graph: GraphFormat2, event: String)
  = {
    println(event)
    val fGraph=graph.filter(f => f, epred = (f: (EdgeFormat2)) => f.attr._2== event)
    fGraph
  }

  def tupleRDDtoPlayersRankingRdd3(games: TupleRDDsFormat, prItr: Int = 10): rankingTupleFormat = {
    val (
      events,
      wPlayers,
      bPlayers,
      result,
      _,
      _,
      _,
      _,
      _,
      _,
      _,
      _
      ) = games
    println("tupleRDDtoPlayersRankingRdd")
    val edgesProperty = getEdgesProperty(events, result, wPlayers, bPlayers)
    val vertexes = getVertexes3(wPlayers, bPlayers).cache

    val edges = edgesProperty.map(mapToEdge3)

    val graph = Graph(vertexes, edges).cache
    val graphClass = filterGraphByEvent(graph, CLASSIC).cache
    val graphBullet = filterGraphByEvent(graph, BULLET).cache
    val graphBlitz = filterGraphByEvent(graph, Blitz).cache
    println("Graphs ready")

    println("Start ranking")

    val innerPRgraph = getPageRang(graph, prItr)
    val outerPRgraph = getPageRang(graph.reverse, prItr)

    val innerPRgraphClass = getPageRang(graphClass, prItr)
    val outerPRgraphClass = getPageRang(graphClass.reverse, prItr)

    val innerPRgraphBullet = getPageRang(graphBullet, prItr)
    val outerPRgraphBullet = getPageRang(graphBullet.reverse, prItr)

    val innerPRgraphBlitz = getPageRang(graphBlitz, prItr)
    val outerPRgraphBlitz = getPageRang(graphBlitz.reverse, prItr)
    println("ranked ranking")
    graphClass.unpersist()
    graphBullet.unpersist()
    graphBlitz.unpersist()
    graph.unpersist()
    println("unpersist")

    (
      vertexes,
      innerPRgraph.map(f => f), innerPRgraphClass.map(f => f), innerPRgraphBullet.map(f => f), innerPRgraphBlitz.map(f => f)
      , outerPRgraph.map(f => f), outerPRgraphClass.map(f => f), outerPRgraphBullet.map(f => f), outerPRgraphBlitz.map(f => f)
    )


  }

  def mergeRows: ((VertexId, (Row, Double))) => (VertexId, Row) = f => (f._1, Row.merge(f._2._1, Row(f._2._2)))

  def joinRankingRDDs(games: TupleRDDsFormat, prItr: Int = 10): RDD[Row] = {
    val rankingData = tupleRDDtoPlayersRankingRdd3(games,prItr)
    rankingData._1.map(f => (f._1, Row(f._2)))
      .join(rankingData._2).map(mergeRows)
      .join(rankingData._3).map(mergeRows)
      .join(rankingData._4).map(mergeRows)
      .join(rankingData._5).map(mergeRows)
      .join(rankingData._6).map(mergeRows)
      .join(rankingData._7).map(mergeRows)
      .join(rankingData._8).map(mergeRows)
      .join(rankingData._9).map(mergeRows)
      .map(f=>f._2)


  }

  def joinedRankingRDDsToDF(sparkSession:SparkSession,games: TupleRDDsFormat, prItr: Int = 10)={
    val RankingRDD=joinRankingRDDs(games,prItr)
    sparkSession.createDataFrame(RankingRDD, StructType(Property.rankinSchema2))

  }


  def playersRanRowRDDtoCSV(): Unit = {

    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val games=pgnETtoTupleRDDs(spark.sparkContext,Property.PGN_FILE)
    val df=joinedRankingRDDsToDF(spark,games)


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }

  def main(args: Array[String]): Unit = {
    playersRanRowRDDtoCSV()

    //
    //    for(i<-args)
    //      println(i)


    //    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
    //    val sc = new SparkContext(conf)
    //
    //    tupleRDDtoNeo4j(sc,Property.PGN_FILE)
    ////    playersRanRowRDDtoCSV(sc, Property.PGN_FILE)


  }

}
