
import java.util.UUID

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.neo4j.spark.cypher.{NameProp, Pattern}
import org.neo4j.spark.utils.Neo4jUtils

object GamesLoader {
  val DIRECTORY = "C:\\tmp_test\\"
  //todo replace
  val PGN_FILE: String = DIRECTORY + "lichess_db_standard_rated_2013-01.pgn.bz2" //todo replace
  val PGN_FILE2: String = DIRECTORY + "lichess_db_standard_rated_2014-07.pgn.bz2" //todo replace


  val csvPath = "D:\\temporary\\tmp.csv"
  //todo replace
  val schemaString: String = "Id Event WightName BlackName Winner WightRating BlackRating" +
    " ECO Opening time-control Date Time Termination GamePlay"
  val schema: Array[String] = schemaString.split(" ")
  val schemaSeq: Seq[StructField] = Seq("Id", "Event", "WightName", "BlackName", "Winner",
    "WightRating", "BlackRating", "ECO", "Opening", "time-control"
    , "Date", "Time", "Termination", "GamePlay").map(feild => StructField(feild, StringType, nullable = false))

  def UUIDLong: String => Long = (s) => UUID.nameUUIDFromBytes(s.getBytes()).getLeastSignificantBits.abs

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
      .set("spark.executor.memory", "4G")
      .set("spark.driver.memory", "4G")
    val sc = new SparkContext(conf)


    //    rowRDDtoCSV(sc,PGN_FILE)

    tupleRDDtoGraphx(sc, PGN_FILE)


  }

  def mapToEdge(t: (String, String, String, String, String, String, String, String,
    String, String, String, String, String)): Edge[String] = {
    val (_, event, wPlayerName, bPlayerName,
    winner, wRating, bRating, eco, opening, timeCtrl,
    date, time, termination) = t

    val wId = UUIDLong(wPlayerName)
    val bId = UUIDLong(bPlayerName)

    var edgeProperty = (event.toString,
      wRating.toString, bRating.toString, eco.toString, opening.toString, timeCtrl.toString,
      date.toString, time.toString, termination)

    if ("B" == winner)
      Edge(bId, wId, event)

    else
      Edge(wId, bId, event)


  }


  //  def mapToEdge(t: (String, String, String, String, String, String, String, String,
  //    String, String, String, String, String)): Edge[String] = {
  //    val (_, event, wPlayerName, bPlayerName,
  //    winner, wRating, bRating, eco, opening, timeCtrl,
  //    date, time, termination) = t
  //
  //    val wId = UUIDLong(wPlayerName)
  //    val bId = UUIDLong(bPlayerName)
  //
  //    var edgeProperty = (event.toString, (
  //      wRating.toString, bRating.toString, eco.toString, opening.toString, timeCtrl.toString,
  //      date.toString, time.toString, termination))
  //
  //    if ("B" == winner)
  //      Edge(bId, wId, event)
  //
  //    else
  //      Edge(wId, bId, event)
  //
  //
  //  }

  //
  def mapRowToVertex(t: (String, String, String, String, String, String, String, String,
    String, String, String, String, String)): (VertexId, String) = {
    val (_, _, wPlayerName, bPlayerName,
    _, _, _, _, _, _,
    _, _, _) = t

    val wId = UUIDLong(wPlayerName)
    val bId = UUIDLong(bPlayerName)


    (wId, wPlayerName)


  }

  def tupleRDDtoGraphx(sc: SparkContext, pgnPath: String = PGN_FILE): Unit = {
    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)

    //    val plr = println(games.map(s => s._3).distinct.count())

    val edges = games.map(mapToEdge)
    val vertexes = games.map(mapRowToVertex)

    val graph = Graph(vertexes, edges)
    //    val verGraph = Graph(vertexes, edges)

//    graph.triangleCount().vertices.map(f=>f._2).foreach(println)

    //    println("connectedComponents: ",graph.connectedComponents().vertices.compute(1).count())
//    println("pageRank: ", graph.pageRank(0.0001).vertices.map(f=>f._2).foreach(println))
    graph.pageRank(0.0001).vertices.map(f=>f._2).foreach(println)

    //    println(graph.vertices.count())
    val neo = Neo4jUtils.executeTxWithRetries()
//    neo.
    //    println(graph.vertices.distinct().count())

    val p = Pattern(NameProp("Player", "identity"), Array(NameProp("event", "text")), NameProp("Player", "identity"))
    //    val p = Pattern(NameProp("Player",), Array(NameProp("event", "text")), NameProp("Player"))
    Some(p.target.asTuple).isDefined



//        neo.saveGraph(sc,graph, "name",relTypeProp=("event", "text"),
//          mainLabelIdProp=Some( p.target.asTuple),
//          secondLabelIdProp = Some( p.target.asTuple),
//           merge = true)


    //    println(graph.numEdges)
    //    println(graph.numVertices)

    //    val vertexes=games.map()


  }


  //  def tester(sc: SparkContext) = {
  //
  //    val g = Neo4jGraph.loadGraph(sc, "Person", Seq("KNOWS"), "Person")
  //    // g: org.apache.spark.graphx.Graph[Any,Int] = org.apache.spark.graphx.impl.GraphImpl@574985d8
  //
  //    g.vertices.count
  //    // res0: Long = 999937
  //
  //    g.edges.count
  //    // res1: Long = 999906
  //
  //    val g2 = PageRank.run(g, 5)
  //
  //    val v = g2.vertices.take(5)
  //    // v: Array[(org.apache.spark.graphx.VertexId, Double)] = Array((185012,0.15), (612052,1.0153273593749998), (354796,0.15), (182316,0.15), (199516,0.38587499999999997))
  //    g2.triplets.foreach(println)
  //    g2.edges.foreach(println)
  //    g2.vertices.foreach(println)
  //    Neo4jGraph.saveGraph(sc, g2, "rank")
  //    // res2: (Long, Long) = (999937,0)
  //
  //    // full syntax example
  //    Neo4jGraph.saveGraph(sc, g, "rank", ("LIKES", "score"), Some(("Person", "name")), Some(("Movie", "title")), merge = true)
  //  }


  def rowRDDtoCSV(sc: SparkContext, pgnPath: String = PGN_FILE): Unit = {


    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = PGNExtractTransform.pgnETtoRowRDD(sc, pgnPath)

    val df = spark.createDataFrame(gameTup, StructType(schemaSeq))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(csvPath)


  }


}
