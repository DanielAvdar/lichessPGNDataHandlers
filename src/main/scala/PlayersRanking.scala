import GamesBigQueryLoader.UUIDLong
import PGNExtractTransform.{pgnETtoTupleRDDs, swapValKey}
import Property._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object PlayersRanking {


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


  private def getPageRang(graph: GraphFormat, prItr: Int): VertexRDD[Double] = {

    graph.staticPageRank(prItr).vertices

  }


  def mapToEdge3(edgesProperty: GraphGameTupleFormat): Edge[GraphGameTupleFormat] = {

    val (_, _, winner, wPlayers, bPlayers) = edgesProperty

    val wId = UUIDLong(wPlayers)
    val bId = UUIDLong(bPlayers)

    if (BLACK == winner)
      Edge(bId, wId, edgesProperty)

    else
      Edge(wId, bId, edgesProperty)

  }


  def mapToProper(edgesProperty: (Long, (((String, String), String), String))): GraphGameTupleFormat = {

    val (gameId, (((events, winner), wPlayers), bPlayers)) = edgesProperty

    (gameId, events, winner, wPlayers, bPlayers)

  }

  def getEdgesProperty(events: RDD[(VertexId, String)], result: RDD[(VertexId, String)],
                       wPlayers: RDD[(VertexId, String)], bPlayers: RDD[(VertexId, String)]): RDD[GraphGameTupleFormat] = {


    val edgesProperty = events
      .join(result)
      .join(wPlayers)
      .join(bPlayers)
      .map(mapToProper)
    edgesProperty
  }


  def filterGraphByEvent(graph: GraphFormat, event: String): GraphFormat
  = {
    println(event)
    val fGraph = graph.filter(f => f, epred = (f: EdgeFormat) => f.attr._2 == event)
    fGraph
  }

  def tupleRDDtoPlayersRankingRdd3(games: TupleRDDsFormat, prItr: Int = 5): rankingTupleFormat = {
    val (
      eventsRDD,
      wPlayersRDD,
      bPlayersRDD,
      resultRDD,
      _, _, _, _, _, _, _, _
      ) = games


    val (
      events,
      wPlayers,
      bPlayers,
      result
      ) = (
      eventsRDD.zipWithIndex().map(swapValKey),
      wPlayersRDD.zipWithIndex().map(swapValKey).cache(),
      bPlayersRDD.zipWithIndex().map(swapValKey) cache(),
      resultRDD.zipWithIndex().map(swapValKey)
    )

    println("tupleRDDtoPlayersRankingRdd")
    val edgesProperty = getEdgesProperty(events, result, wPlayers, bPlayers)
    val vertexes = getVertexes3(wPlayers, bPlayers).cache
    wPlayers.unpersist()
    bPlayers.unpersist()
    val edges = edgesProperty.map(mapToEdge3)

    val graph = Graph(vertexes, edges).cache
    val graphClass = filterGraphByEvent(graph, CLASSIC).cache
    val graphBullet = filterGraphByEvent(graph, BULLET).cache
    val graphBlitz = filterGraphByEvent(graph, BLITZ).cache
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
    println("Ranked")
    graphClass.unpersist()
    graphBullet.unpersist()
    graphBlitz.unpersist()
    graph.unpersist()
    println("Unpersist")

    (
      vertexes,
      innerPRgraph.map(f => f), innerPRgraphClass.map(f => f), innerPRgraphBullet.map(f => f), innerPRgraphBlitz.map(f => f)
      , outerPRgraph.map(f => f), outerPRgraphClass.map(f => f), outerPRgraphBullet.map(f => f), outerPRgraphBlitz.map(f => f)
    )


  }

  def mergeRows: ((VertexId, (Row, Double))) => (VertexId, Row) = f => (f._1, Row.merge(f._2._1, Row(f._2._2)))

  def joinRankingRDDs(games: TupleRDDsFormat, prItr: Int = 5): RDD[Row] = {
    val rankingData = tupleRDDtoPlayersRankingRdd3(games, prItr)
    rankingData._1.map(f => (f._1, Row(f._2)))
      .join(rankingData._2).map(mergeRows)
      .join(rankingData._3).map(mergeRows)
      .join(rankingData._4).map(mergeRows)
      .join(rankingData._5).map(mergeRows)
      .join(rankingData._6).map(mergeRows)
      .join(rankingData._7).map(mergeRows)
      .join(rankingData._8).map(mergeRows)
      .join(rankingData._9).map(mergeRows)
      .map(f => f._2)


  }

  def joinedRankingRDDsToDF(sparkSession: SparkSession, games: TupleRDDsFormat, prItr: Int = 5): DataFrame = {
    val RankingRDD = joinRankingRDDs(games, prItr)
    sparkSession.createDataFrame(RankingRDD, StructType(Property.rankinSchema2))

  }


  def playersRanRowRDDtoCSV(): Unit = {

    val sparkSession = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val games = pgnETtoTupleRDDs(sparkSession.sparkContext, Property.PGN_FILE2)
    val df = joinedRankingRDDsToDF(sparkSession, games)


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }

  //test
  def main(args: Array[String]): Unit = {
    playersRanRowRDDtoCSV()

  }

}
