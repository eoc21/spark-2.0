package eoc21.nlp

/**
  * Created by edwardcannon on 06/02/2017.
  */

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.{Params, ParamMap, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{IntegerType, StructField, StringType, StructType}

trait SimpleIndexerParams extends Params {
  final val inputCol = new Param[String](this, "inputCol","The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
}

class SimpleIndexer(override val uid: String) extends Estimator[SimpleIndexerModel] with SimpleIndexerParams {

  def setInputCol(value: String) = set(inputCol, value)

  def setOutputCol(value: String) = set(outputCol, value)

  def this() = this(Identifiable.randomUID("simpleindexer"))

  override def copy(extra: ParamMap): SimpleIndexer = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType){
      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
    }
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def fit(dataset: Dataset[_]): SimpleIndexModel = {
    import dataset.sparkSession.implicits._
    val words = dataset.select((dataset($(inputCol)).as[String]).distinct.collect())
    new SimpleIndexModel(uid, words)
  }
}
