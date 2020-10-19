
import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._


object Property {


  val BLACK = "B"
  val WHITE = "W"
  val DRAW = "D"
  val CLASSIC="Classical"
  val BULLET="Bullet"
  val Blitz="Blitz"

  val DIRECTORY = "C:\\tmp_test\\"
  //todo replace
  val PGN_FILE: String = DIRECTORY + "lichess_db_standard_rated_2013-01.pgn.bz2" //todo replace
  //  val PGN_FILE2: String = DIRECTORY + "lichess_db_standard_rated_2014-07.pgn.bz2" //todo replace


  val csvPath: String = DIRECTORY + "temporary\\"
  //todo replace
  val mapper = Map("name" -> StringType,
    "rating" -> IntegerType, "WinLosRatio" -> DoubleType, "TotalPR" -> DoubleType,
    "numOfGames" -> IntegerType, "numOfGamesRapid" -> IntegerType,
    "numOfGamesClass" -> IntegerType,
    "numOfGamesBullet" -> IntegerType, "numOfGamesBlitz" -> IntegerType)
    .withDefault((_: String) => DoubleType)
  val mapper2 = Map("name" -> StringType)
    .withDefault((_: String) => DoubleType)
  val rankinSchema: Seq[StructField] = Seq(
    "name", //0
    "rating", //1

    "TotalPRRatio", //4
    "numOfGames", //2
    "WinLosRatio", //3

    "PRRatioClass", //7
    "numOfGamesClass", //5
    "winLosRatioClass", //6

    "PRRatioBullet", //10
    "numOfGamesBullet", //8
    "winLosRatioBullet", //9

    "PRRatioBlitz", //13
    "numOfGamesBlitz", //11
    "winLosRatioBlitz", //12

    "PRRatioRapid", //16
    "numOfGamesRapid", //14
    "winLosRatioRapid" //15

  ).map(field => StructField(field, mapper(field), nullable = field != "name"))

  val rankinSchema2: Seq[StructField] = Seq(
    "name", //0

    "innerPRgraph", //4
    "innerPRgraphClass", //2
    "innerPRgraphBullet", //3
    "innerPRgraphBlitz", //7

    "outerPRgraph", //5
    "outerPRgraphClass", //6
    "outerPRgraphBullet", //10
    "outerPRgraphBlitz", //8


  ).map(field => StructField(field, mapper2(field), nullable = field != "name"))


  val gameSchema: Seq[StructField] = Seq(
    //    "Id",
    "Event",
    "WightName",
    "BlackName",
    "Winner",
    "WightRating",
    "BlackRating",
    "ECO",
    "Opening",
    "time-control",
    "Date",
    "Time",
    "Termination"
  )
    .map(field => StructField(field, StringType, nullable = false))

  type EdgeTripletFormat = EdgeTriplet[String, (String, String, String, String,
    String, String, String, String, String, String, String, String)]
  type PlayerRatingAndNameFormat = ((VertexId, String), Int)

  type GameTupleFormat = (String, String, String, String, String, String, String, String,
    String, String, String, String, String)
  type GameTupleFormat2 = (Long, String, String, String, String)
  type GameJoinFormat = (Long, (((((((((((String, String), String), String), String),
    String), String), String), String),
    String), String), String))
type rankingTupleFormat=   (RDD[(VertexId, String)], RDD[(VertexId, Double)], RDD[(VertexId, Double)], RDD[(VertexId, Double)],
  RDD[(VertexId, Double)], RDD[(VertexId, Double)], RDD[(VertexId, Double)], RDD[(VertexId, Double)], RDD[(VertexId, Double)])

    type EdgeFormat = Edge[(String, String, String, String, String,
    String, String, String, String, String, String, String)]

  type EdgeFormat2 = Edge[(Long, String, String, String, String)]

  type GraphFormat = Graph[String, (String, String, String,
    String, String, String, String, String, String, String, String, String)]
  type GraphFormat2 = Graph[String, (Long, String, String, String, String)]
  type TupleRDDsFormat =  (RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)],
    RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)],
    RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)],
    RDD[(Long, String)])


}
