import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
object RankingEvaluation {

  def rowsRddToLabeledRatingPointsRdd(rowsRdd:RDD[Row]):RDD[LabeledPoint]={

    def rowMapper(row: Row)={
      LabeledPoint(row(1).asInstanceOf[Double],Vectors.dense(row(2).asInstanceOf[Double],
        row(3).asInstanceOf[Double],row(4).asInstanceOf[Double]))
    }

    rowsRdd.map(rowMapper)




    }
  def rowsRddToLabeledPageRankPointsRdd(rowsRdd:RDD[Row]):RDD[LabeledPoint]={

    def rowMapper(row: Row)={
      LabeledPoint(row(4).asInstanceOf[Double],Vectors.dense(row(2).asInstanceOf[Double],
        row(3).asInstanceOf[Double],row(1).asInstanceOf[Double]))
    }

    rowsRdd.map(rowMapper)



  }

  def main(args: Array[String]): Unit = {
    PlayersRankingLoader
  }


}
