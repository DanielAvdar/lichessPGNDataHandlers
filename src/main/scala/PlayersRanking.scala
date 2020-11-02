import GamesBigQueryLoader.UUIDLong
import PGNExtractTransform.{pgnETtoTupleRDDs, swapValKey}
import Property._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER


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

  def tupleRDDtoPlayersRankingRdd3(games: TupleRDDsFormat, prItr: Int,onEvent:String=ALL_GAMES): rankingTupleFormat = {
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
      eventsRDD.zipWithIndex().map(swapValKey).filter(f => Property.filterValidator(f._2)),
      wPlayersRDD.zipWithIndex().map(swapValKey).persist(MEMORY_AND_DISK_SER),
      bPlayersRDD.zipWithIndex().map(swapValKey).persist(MEMORY_AND_DISK_SER),
      resultRDD.zipWithIndex().map(swapValKey).filter(f => f._2 != DRAW)
    )
    pgnFile.unpersist()

    println("tupleRDDtoPlayersRankingRdd")
    val edgesProperty = getEdgesProperty(events, result, wPlayers, bPlayers)
    val vertexes = getVertexes3(wPlayers, bPlayers).persist(MEMORY_AND_DISK_SER)

    val edges = edgesProperty.map(mapToEdge3).repartition(150).persist(MEMORY_AND_DISK_SER)

    val graph = Graph(vertexes, edges).cache()
    val innerPRgraph = getPageRang(graph, prItr)
    wPlayersRDD.unpersist()
    bPlayersRDD.unpersist()
    val outerPRgraph = getPageRang(graph.reverse, prItr)

    val graphClass = filterGraphByEvent(graph, CLASSIC)
    val innerPRgraphClass = getPageRang(graphClass, prItr)
    val outerPRgraphClass = getPageRang(graphClass.reverse, prItr)

    val graphBullet = filterGraphByEvent(graph, BULLET)
    val innerPRgraphBullet = getPageRang(graphBullet, prItr)
    val outerPRgraphBullet = getPageRang(graphBullet.reverse, prItr)

    val graphBlitz = filterGraphByEvent(graph, BLITZ)
    val innerPRgraphBlitz = getPageRang(graphBlitz, prItr)
    val outerPRgraphBlitz = getPageRang(graphBlitz.reverse, prItr)

    val graphRapid = filterGraphByEvent(graph, RAPID)
    val innerPRgraphRapid = getPageRang(graphRapid, prItr)
    val outerPRgraphRapid = getPageRang(graphRapid.reverse, prItr)













    println("Ranked")
    edges.unpersist()
    println("Unpersist edges")

    val res = (
      vertexes,
      innerPRgraph,
      innerPRgraphClass,
      innerPRgraphBullet,
      innerPRgraphBlitz,
      innerPRgraphRapid,

      outerPRgraph,
      outerPRgraphClass,
      outerPRgraphBullet,
      outerPRgraphBlitz,
      outerPRgraphRapid

    )
    res


  }

  def mergeRows: ((VertexId, (Row, Double))) => (VertexId, Row) = f => (f._1, Row.merge(f._2._1, Row(f._2._2)))

  def joinRankingRDDs(games: TupleRDDsFormat, prItr: Int): RDD[Row] = {
    val rankingData = tupleRDDtoPlayersRankingRdd3(games, prItr)
    val res = rankingData._1.map(f => (f._1, Row(f._2)))
      .join(rankingData._2).map(mergeRows)
      .join(rankingData._3).map(mergeRows)
      .join(rankingData._4).map(mergeRows)
      .join(rankingData._5).map(mergeRows)
      .join(rankingData._6).map(mergeRows)
      .join(rankingData._7).map(mergeRows)
      .join(rankingData._8).map(mergeRows)
      .join(rankingData._9).map(mergeRows)
      .join(rankingData._10).map(mergeRows)
      .join(rankingData._11).map(mergeRows)
      .map(f => f._2)
//    rankingData._1.unpersist()
//    println("Unpersist vertexes")

    res



  }

  def joinedRankingRDDsToDF(sparkSession: SparkSession, games: TupleRDDsFormat, prItr: Int = 5): DataFrame = {
    val RankingRDD = joinRankingRDDs(games, prItr)
    sparkSession.createDataFrame(RankingRDD, StructType(Property.rankinSchema2))

  }


  def playersRanRowRDDtoCSVTest(): Unit = {

    val sparkSession = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    val games = pgnETtoTupleRDDs(sparkSession.sparkContext, Property.PGN_FILE, PGNExtractTransform.rankingUnUsedDataFilter)
    val df = joinedRankingRDDsToDF(sparkSession, games)


    df.write.format("csv").option("header", value = true).mode("overwrite").save(Property.csvPath)


  }


  //test
  def main(args: Array[String]): Unit = {
    playersRanRowRDDtoCSVTest()

  }

}
