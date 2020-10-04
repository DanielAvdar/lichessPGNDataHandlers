
import java.util.UUID

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

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
    , "Date", "Time", "Termination", "GamePlay").map(feild => StructField(feild, StringType, nullable = false))

  def UUIDLong: String => Long = (s) => UUID.nameUUIDFromBytes(s.getBytes()).getLeastSignificantBits.abs

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")
      .set("spark.executor.memory", "4G")
      .set("spark.driver.memory", "4G")
    val sc = new SparkContext(conf)


    //    rowRDDtoCSV(sc,PGN_FILE)
    playersRowRDDtoCSV(sc, PGN_FILE)

    //    tupleRDDtoGraphx(sc, PGN_FILE)


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


  def mapRowToVertex(t: (String, String, String, String, String, String, String, String,
    String, String, String, String, String)): (VertexId, String) = {
    val (_, _, wPlayerName, bPlayerName,
    _, _, _, _, _, _,
    _, _, _) = t

    val wId = UUIDLong(wPlayerName)
    val bId = UUIDLong(bPlayerName)


    (wId, wPlayerName)


  }

  //  def playerRating[a,b](t:EdgeTriplet)={
  //
  //  }

  def mapWPlayerRating(f: EdgeTriplet[String, (String, String, String, String,
    String, String, String, String, String, String, String, String)]): ((VertexId, String), Int) = {
    var rating = 0
    if (f.attr._5 != "?") {
      rating = f.attr._5.toInt
    }


    if (f.attr._4 == "B") {
      f.attr._5
      ((f.dstId, f.dstAttr), rating)
    } else
      ((f.srcId, f.srcAttr), rating)

  }

  def mapBPlayerRating(f: EdgeTriplet[String, (String, String, String, String,
    String, String, String, String, String, String, String, String)]): ((VertexId, String), Int) = {
    var rating = 0
    if (f.attr._6 != "?") {
      rating = f.attr._6.toInt
    }


    if (f.attr._4 == "B")

      ((f.srcId, f.srcAttr), rating)
    else
      ((f.dstId, f.dstAttr), rating)

  }


  def tupleRDDtoGraphx(sc: SparkContext, pgnPath: String = PGN_FILE): RDD[Row] = {
    val games = PGNExtractTransform.pgnETtoTuple(sc, pgnPath)


    val edges = games.map(mapToEdge).filter(f => f.attr._4 != "D")
    val vertexes = games.map(mapRowToVertex)

    val graph = Graph(vertexes, edges)
    val graphClass = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("C")))
    val graphBullet = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Bullet")))
    val graphBlitz = Graph(vertexes, edges.filter(f => f.attr._1.startsWith("Blitz")))
    val prItr = 30
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

    val wPlayerRating = graph.triplets.map(mapWPlayerRating)
    val bPlayerRating = graph.triplets.map(mapBPlayerRating)
    wPlayerRating //.foreach(println)
    bPlayerRating // .foreach(println)
    val PlayerRating = bPlayerRating.union(wPlayerRating).groupByKey()
      .aggregateByKey(0)((a, b) => a + b.sum / b.size, (a, b) => a + b)
      .map(f => (f._1._1, (f._1._2, f._2)))
    //      .foreach(println)
    val finalData = PlayerRating
      .join(innerPRgraph)
      .join(outerPRgraph)
      .join(innerPRgraphClass)
      .join(outerPRgraphClass)
      .join(innerPRgraphBullet)
      .join(outerPRgraphBullet)
      .join(innerPRgraphBlitz)
      .join(outerPRgraphBlitz)
      .join(numOfGames)
      .join(numOfWins)
      .join(numOfLoses)

    finalData.map(playerTuples).map(row)
  }


  type pJoinFormat = (VertexId, ((((((((((((String, Int), Double), Double), Double), Double), Double),
    Double), Double), Double), Int), Int), Int))
  type pTupleFormat = (String, Int, Int, Double, Double, Double, Double, Double)

  def playerTuples(f: pJoinFormat): pTupleFormat= {

    var (_, ((((((((((((name, rating), innerPRgraph), outerPRgraph),
    innerPRgraphClass), outerPRgraphClass), innerPRgraphBullet), outerPRgraphBullet), innerPRgraphBlitz), outerPRgraphBlitz), numOfGames),
    numOfWins), numOfLoses)) = f
    if (numOfLoses == 0)
      numOfLoses = 1
    (name, rating, numOfGames, numOfWins.toDouble / numOfLoses.toDouble, outerPRgraph / innerPRgraph,
      outerPRgraphClass / innerPRgraphClass, outerPRgraphBullet / innerPRgraphBullet, outerPRgraphBlitz / innerPRgraphBlitz)


  }

  def row(f: pTupleFormat): Row = {
    val (a, b, c, d, e, ff, g, i) = f
    val gameRow = sql.Row(a, b, c, d, e, ff, g, i)
    gameRow
  }

  def rowRDDtoCSV(sc: SparkContext, pgnPath: String = PGN_FILE): Unit = {


    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = PGNExtractTransform.pgnETtoRowRDD(sc, pgnPath)

    val df = spark.createDataFrame(gameTup, StructType(schemaSeq))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(csvPath)


  }

  def playersRowRDDtoCSV(sc: SparkContext, pgnPath: String = PGN_FILE): Unit = {
    val mapper = Map("name" -> StringType,
      "rating" -> IntegerType, "WinLosRatio" -> DoubleType, "TotalPR" -> DoubleType,
      "GameNum" -> IntegerType).withDefault((a:String)=>DoubleType)

    val spark = new SparkSession.Builder().master("local[9]").appName("lichess").getOrCreate()
    val gameTup = tupleRDDtoGraphx(sc, pgnPath)
    val schemaSeq2 = Seq("name", "rating", "GameNum", "WinLosRatio", "TotalPR", "ClassicPR",
      "BulletPR", "BlitzPR").map(feild => StructField(feild, mapper(feild), nullable = true))

    val df = spark.createDataFrame(gameTup, StructType(schemaSeq2))


    df.write.format("csv").option("header", value = true).mode("overwrite").save(csvPath)


  }
}
