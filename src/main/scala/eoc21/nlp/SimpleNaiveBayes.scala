package eoc21.nlp

import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.linalg.{Vectors, DenseMatrix}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Row, Dataset}
import org.apache.spark.sql.functions.{udf, col, count}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.param.ParamMap

/**
  * Created by edwardcannon on 06/02/2017.
  */
class SimpleNaiveBayes(val uid: String)
  extends Classifier[Vector, SimpleNaiveBayes, SimpleNaiveBayesModel] {

  def this() = this(Identifiable.randomUID("simple-naive-bayes"))

  override def train(ds: Dataset[_]): SimpleNaiveBayesModel = {
    import ds.sparkSession.implicits._
    ds.cache()

    //Compute number of documents
    val numDocs = ds.count

    val numClasses = getNumClasses(ds)
    val numFeatures: Integer = ds.select(col($(featuresCol))).head
      .get(0).asInstanceOf[Vector].size

    val groupedByLabel = ds.select(col($(labelCol)).as[Double]).groupByKey( x => x)

    val classCounts = groupedByLabel.agg(count("*").as[Long]).sort(col("value")).collect().toMap

    val df = ds.select(col($(labelCol)).cast(DoubleType),col($(featuresCol)))

    val labelCount: Dataset[LabeledToken] = df.flatMap{
      case Row(label: Double, features: Vector) =>
        features.toArray.zip(Stream from 1)
          .filter{vIdx => vIdx._2 == 1.0}
          .map{case (v, idx) => LabeledToken(label, idx)}
    }

    val aggregatedCounts: Array[((Double, Integer), Long)] = labelCount
      .groupByKey(x => (x.label, x.index))
      .agg(count("*").as[Long]).collect()

    val theta = Array.fill(numClasses)(new Array[Double](numFeatures))
    val piLogDenom = math.log(numDocs + numClasses)
    val pi = classCounts.map({case(_, cc)
    => math.log(cc.toDouble) - piLogDenom
    }.toArray)

    aggregatedCounts.foreach{case ((label, featureIndex), count) =>
    val thetaLogDenom = math.log(
      classCounts.get(label).map(_.toDouble).getOrElse(0.0) + 2.0)
      theta(label.toInt)(featureIndex) = math.log(count + 1.0) - thetaLogDenom
    }

    ds.unpersist()

    new SimpleNaiveBayesModel(uid, numClasses, numFeatures, Vectors.dense(pi),
      new DenseMatrix(numClasses, theta(0).length, theta.flatten, true))
  }

  override  def copy(extra: ParamMap) = {
    defaultCopy(extra)
  }

}

case class SimpleNaiveBayesModel(
  override val uid: String,
  override val numClasses: Int,
  override val numFeatures: Int,
  val pi: Vector,
  val theta: DenseMatrix) extends
ClassificationModel[Vector, SimpleNaiveBayesModel]{
  override def copy(extra: ParamMap) = {
    defaultCopy(extra)
  }

  val negThetaArray = theta.values.map(v => math.log(1.0 - math.exp(v)))
  val negTheta = new DenseMatrix(numClasses, numFeatures, negThetaArray, true)
  val thetaMinusNegThetaArray = theta.values.zip(negThetaArray).map{case (v, nv) => v -nv}
  val thetaMinusNegTheta = new DenseMatrix(numClasses, numFeatures, thetaMinusNegThetaArray, true)
  val onesVec = Vectors.dense((Array.fill(theta.numCols)(1.0)))
  val negThetaSum: Array[Double] = negTheta.multiply(onesVec).toArray

  def predictRaw(features: Vector): Vector = {
    Vectors.dense(thetaMinusNegTheta.multiply(features).toArray.zip(pi.toArray)
      .map{case (x,y) => x +y}.zip(negThetaSum).map{case (x,y) => x + y})
  }
}

case class LabeledToken(label: Double, idx: Int)
