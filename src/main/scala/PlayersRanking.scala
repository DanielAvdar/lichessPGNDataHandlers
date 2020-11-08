import GamesBigQueryLoader.UUIDLong
import PGNExtractTransform.{pgnETtoTupleRDDs, swapValKey}
import Property._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel.DISK_ONLY


object PlayersRanking {


  def MapTupToVertex(name: String): (VertexId, String) = {

    val bId = UUIDLong(name)


    (bId, name)

  }

  private def getVertexes(wPlayers: RDD[(Long, String)], bPlayers: RDD[(Long, String)]) = {
    wPlayers
      .map(f => f._2)
      .distinct()
      .union(bPlayers
        .map(f => f._2)
        .distinct())
      .distinct
      .map(MapTupToVertex)

  }


  private def getPageRank(graph: GraphFormat, prItr: Int): VertexRDD[Double] = {

    graph.staticPageRank(prItr).vertices

  }


  def mapToEdge(edgesProperty: GraphGameTupleFormat): Edge[GraphGameTupleFormat] = {

    val (_, _, winner, wPlayers, bPlayers) = edgesProperty

    val wId = UUIDLong(wPlayers)
    val bId = UUIDLong(bPlayers)

    if (BLACK == winner)
      Edge(bId, wId, edgesProperty)

    else
      Edge(wId, bId, edgesProperty)

  }


  def mapToProperTuple(edgesProperty: (Long, (((String, String), String), String))): GraphGameTupleFormat = {

    val (gameId, (((events, winner), wPlayers), bPlayers)) = edgesProperty

    (gameId, events, winner, wPlayers, bPlayers)

  }

  def getEdgesProperty(events: RDD[(VertexId, String)], result: RDD[(VertexId, String)],
                       wPlayers: RDD[(VertexId, String)], bPlayers: RDD[(VertexId, String)]): RDD[GraphGameTupleFormat] = {


    val edgesProperty = events
      .join(result)
      .join(wPlayers)
      .join(bPlayers)
      .map(mapToProperTuple)
    edgesProperty
  }


  def filterGraphByEvent(graph: GraphFormat, event: String): GraphFormat
  = {
    println(event)
    val fGraph = graph.filter(f => f, epred = (f: EdgeFormat) => f.attr._2 == event)
    fGraph
  }

  def tupleRDDtoPlayersRankingRdd3(games: TupleRDDsFormat, prItr: Int, onEvent: String = ALL_GAMES): rankingTupleFormat = {
    val (
      pgnFile,
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
      eventsRDD.zipWithIndex().map(swapValKey)
        .filter(f => f._2.startsWith(onEvent) || (onEvent == ALL_GAMES && Property.filterValidator(f._2)))
        .persist(DISK_ONLY),


      wPlayersRDD.zipWithIndex().map(swapValKey).persist(DISK_ONLY),
      bPlayersRDD.zipWithIndex().map(swapValKey).persist(DISK_ONLY),
      resultRDD.zipWithIndex().map(swapValKey).filter(f => f._2 != DRAW).persist(DISK_ONLY)
    )
    pgnFile.unpersist()

    println("tupleRDDtoPlayersRankingRdd")
    val edgesProperty = getEdgesProperty(events, result, wPlayers, bPlayers)
    val vertexes = getVertexes(wPlayers, bPlayers).persist(DISK_ONLY)

    val edges = edgesProperty.map(mapToEdge).repartition(150).persist(DISK_ONLY)

    val graph = Graph(vertexes, edges).cache()
    val innerPRgraph = getPageRank(graph, prItr)
    events.unpersist()
    wPlayers.unpersist()
    bPlayers.unpersist()
    result.unpersist()
    val outerPRgraph = getPageRank(graph.reverse, prItr)
    edges.unpersist()
    graph.unpersist()


    println("Ranked")


    val res = (
      vertexes,
      innerPRgraph,


      outerPRgraph,


    )
    res


  }

  def mergeRows: ((VertexId, (Row, Double))) => (VertexId, Row) = f => (f._1, Row.merge(f._2._1, Row(f._2._2)))

  def joinRankingRDDs(games: TupleRDDsFormat, prItr: Int, onEvent: String): RDD[Row] = {
    val rankingData = tupleRDDtoPlayersRankingRdd3(games, prItr, onEvent)
    val res = rankingData._1.map(f => (f._1, Row(f._2)))
      .join(rankingData._2).map(mergeRows)
      .join(rankingData._3).map(mergeRows)

      .map(f => f._2)


    res


  }

  def joinedRankingRDDsToDF(sparkSession: SparkSession, games: TupleRDDsFormat, prItr: Int = 5, onEvent: String): DataFrame = {
    val RankingRDD = joinRankingRDDs(games, prItr, onEvent)
    sparkSession.createDataFrame(RankingRDD, StructType(Property.rankinSchema2))

  }


  def playersRanRowRDDtoCSVTest(): Unit = {

    val sparkSession = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val games = pgnETtoTupleRDDs(sparkSession.sparkContext, Property.PGN_FILE, PGNExtractTransform.rankingUnUsedDataFilter)
    val df = joinedRankingRDDsToDF(sparkSession, games, onEvent = ALL_GAMES)


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }


  //test
  def main(args: Array[String]): Unit = {
    playersRanRowRDDtoCSVTest()

  }

}
