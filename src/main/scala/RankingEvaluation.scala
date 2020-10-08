
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
//import org.apache.spark.ml.regression.{LabeledPoint,LassoModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

object RankingEvaluation {

  def rowsRddToLabeledRatingPointsRdd(rowsRdd: RDD[Row]): RDD[LabeledPoint] = {

    def rowMapper(row: Row) = {
      LabeledPoint(row(1).asInstanceOf[Int],
        Vectors.dense(
          row(2).asInstanceOf[Int],
          row(3).asInstanceOf[Double],
          row(4).asInstanceOf[Double],

          row(5).asInstanceOf[Int],
          row(6).asInstanceOf[Double],

          row(8).asInstanceOf[Int],
          row(9).asInstanceOf[Double],

          row(11).asInstanceOf[Int],
          row(12).asInstanceOf[Double]
        ))
    }

    rowsRdd.map(rowMapper)


  }

  def rowsRddToLabeledPageRankPointsRdd(rowsRdd: RDD[Row]): RDD[LabeledPoint] = {

    def rowMapper(row: Row) = {
      LabeledPoint(row(4).asInstanceOf[Double],
        Vectors.dense(
          row(1).asInstanceOf[Int],

          row(2).asInstanceOf[Int],
          row(3).asInstanceOf[Double],

          row(5).asInstanceOf[Int],
          row(6).asInstanceOf[Double],

          row(8).asInstanceOf[Int],
          row(9).asInstanceOf[Double],

          row(11).asInstanceOf[Int],
          row(12).asInstanceOf[Double]
        ))
    }

    rowsRdd.map(rowMapper)


  }

  def reggression1(labeledPoint: RDD[LabeledPoint]): LinearRegressionModel = {
    val splits = labeledPoint.randomSplit(Array(0.7, 0.3))
    val training = splits(0).cache()
    val test = splits(1)
    val numIterations = 200
    val stepSize = 0.0000001
    val model = LinearRegressionWithSGD.train(training, numIterations, stepSize) //    model.fit(,)
    //    model.clearThreshold()
    //    val scoreAndLabels = test.map { point =>
    //      val score = model.predict(point.features)
    //      (score, point.label)
    //    }
    //    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    //    val auROC = metrics.areaUnderROC()
    //    println(s"Area under ROC = $auROC")
    //    println(metrics.scoreAndLabels)
    model
    //   training.foreach( point =>
    //     println((point.label,  model.predict(point.features)))
    //    )
    //    1

  }
  def reggression2(labeledPoint: RDD[LabeledPoint]): LinearRegressionModel = {
    val splits = labeledPoint.randomSplit(Array(0.7, 0.3))
    val training = splits(0).cache()
    val test = splits(1)
    val numIterations = 200
    val stepSize = 0.0000001
    val model = LinearRegressionWithSGD.train(training, numIterations, stepSize) //    model.fit(,)
    //    model.clearThreshold()
    //    val scoreAndLabels = test.map { point =>
    //      val score = model.predict(point.features)
    //      (score, point.label)
    //    }
    //    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    //    val auROC = metrics.areaUnderROC()
    //    println(s"Area under ROC = $auROC")
    //    println(metrics.scoreAndLabels)
    model
    //   training.foreach( point =>
    //     println((point.label,  model.predict(point.features)))
    //    )
    //    1

  }



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[12]").setAppName("lichess")

    val sc = new SparkContext(conf)

    val rankedPlayers = PlayersRankingLoader.tupleRDDtoPlayersRankingRdd(sc)


    val rankedPlayersRatL = rowsRddToLabeledPageRankPointsRdd(rankedPlayers)
    val modelPredictPR = reggression1(rankedPlayersRatL)
    rankedPlayersRatL.foreach(point =>
      println(("pr", point.label, modelPredictPR.predict(point.features)))
    )

//    val rankedPlayersPRL = rowsRddToLabeledRatingPointsRdd(rankedPlayers)
//    val modelPredictRat = reggression1(rankedPlayersPRL)
//    //    rankedPlayersPRL.foreach(println)
//    rankedPlayersPRL.foreach(point =>
//      println(("rating", point.label, modelPredictRat.predict(point.features)))
//    )

  }


}
