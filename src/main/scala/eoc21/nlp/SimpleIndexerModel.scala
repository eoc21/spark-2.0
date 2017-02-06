package eoc21.nlp

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{udf, col}

/**
  * Created by edwardcannon on 06/02/2017.
  */
class SimpleIndexerModel(override val uid: String, words: Array[String]) extends Model[SimpleIndexerModel] with SimpleIndexerParams {

  override def copy(extra: ParamMap): SimpleIndexerModel = {
    defaultCopy(extra)
  }

  private val labelToIndex: Map[String, Double] = words.zipWithIndex.map {
    case (x, y) => (x, y.toDouble)
  }.toMap

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
    }
    schema.add(StructField($(outputCol), IntegerType, false))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val indexer = udf { label: String => labelToIndex(label) }
    dataset.select(col("*"),
    indexer(dataset($(inputCol)).cast(StringType).as($(outputCol)))
  }


}
