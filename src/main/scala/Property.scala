
import org.apache.spark.graphx.{Edge, EdgeTriplet, VertexId}
import org.apache.spark.sql.types._





object Property {


  val BLACK="B"
  val WHITE="W"
  val DRAW="D"

  val DIRECTORY = "C:\\tmp_test\\"
  //todo replace
  val PGN_FILE: String = DIRECTORY + "lichess_db_standard_rated_2013-01.pgn.bz2" //todo replace
  val PGN_FILE2: String = DIRECTORY + "lichess_db_standard_rated_2014-07.pgn.bz2" //todo replace


  val csvPath: String = DIRECTORY + "temporary\\tmp.csv"
  //todo replace
  val mapper = Map("name" -> StringType,
    "rating" -> IntegerType, "WinLosRatio" -> DoubleType, "TotalPR" -> DoubleType,
    "numOfGames" -> IntegerType, "numOfGamesClass" -> IntegerType,
    "numOfGamesBullet" -> IntegerType, "numOfGamesBlitz" -> IntegerType)
    .withDefault((_: String) => DoubleType)
  val rankinSchema: Seq[StructField] = Seq("name", "rating",


    "numOfGames",
    "WinLosRatio",
    "TotalPRRatio",

    "numOfGamesClass",
    "winLosRatioClass",
    "PRRatioClass",

    "numOfGamesBullet",
    "winLosRatioBullet",
    "PRRatioBullet",

    "numOfGamesBlitz",
    "winLosRatioBlitz",
    "PRRatioBlitz",
  ).map(field => StructField(field, mapper(field), nullable = field != "name"))


  val gameSchema: Seq[StructField] = Seq("Id", "Event", "WightName",
    "BlackName", "Winner", "WightRating", "BlackRating", "ECO",
    "Opening", "time-control", "Date", "Time", "Termination")
    .map(field => StructField(field, StringType, nullable = false))

  type EdgeTripletFormat = EdgeTriplet[String, (String, String, String, String,
    String, String, String, String, String, String, String, String)]
  type PlayerRatingAndNameFormat = ((VertexId, String), Int)

  type GameTupleFormat = (String, String, String, String, String, String, String, String,
    String, String, String, String, String)

  type GameJoinFormat = (Long, (((((((((((String, String), String), String), String),
    String), String), String), String),
    String), String), String))

  type EdgeFormat = Edge[(String, String, String, String, String,
    String, String, String, String, String, String, String)]





}
