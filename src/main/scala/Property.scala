
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._


object Property {


  val BLACK = "B"
  val WHITE = "W"
  val DRAW = "D"

  val CLASSIC = "Classical"
  val BULLET = "Bullet"
  val BLITZ = "Blitz"
  val RAPID = "Rapid"

  val ALL_GAMES = "ALL_GAMES"

  def filterValidator(event: String): Boolean = {
    event == CLASSIC || event == BULLET || event == BLITZ || event == RAPID
  }

  val BUCKET = "pgn_contaner"
  val DATASET = "lichessDS"

  val DIRECTORY = "C:\\tmp_test\\"

  val PGN_FILE: String = DIRECTORY + "lichess_db_standard_rated_2013-01.pgn.bz2"
  val PGN_FILE2: String = DIRECTORY + "lichess_db_standard_rated_2014-07.pgn.bz2"


  val csvPath: String = DIRECTORY + "temporary\\"


  val mapper = Map("name" -> StringType)
    .withDefault((_: String) => DoubleType)

  val rankinSchema2: Seq[StructField] = Seq(
    "name",

    "innerPRgraph",


    "outerPRgraph",


  ).map(field => StructField(field, mapper(field), nullable = field != "name"))


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


  type GameTupleFormat = (String, String, String, String, String, String, String, String,
    String, String, String, String, String)
  type GraphGameTupleFormat = (Long, String, String, String, String)
  type GameJoinFormat = (Long, (((((((((((String, String), String), String), String),
    String), String), String), String),
    String), String), String))
  type rankingTupleFormat = (RDD[(VertexId, String)],
    RDD[(VertexId, Double)], RDD[(VertexId, Double)])


  type EdgeFormat = Edge[(Long, String, String, String, String)]


  type GraphFormat = Graph[String, (Long, String, String, String, String)]
  type TupleRDDsFormat = (RDD[String], RDD[String], RDD[String], RDD[String], RDD[String], RDD[String],
    RDD[String], RDD[String], RDD[String], RDD[String], RDD[String], RDD[String], RDD[String])
  type TupleRDDsWithIndexFormat = (RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)],
    RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)],
    RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)], RDD[(Long, String)])


}
