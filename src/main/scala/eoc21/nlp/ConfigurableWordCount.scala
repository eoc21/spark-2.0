package eoc21.nlp

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{udf, col}

/**
  * Created by edwardcannon on 06/02/2017.
  */
class ConfigurableWordCount(override val uid: String) extends Transformer {
  final val inputCol = new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this() = this(Identifiable.randomUID("configurablewordcount"))

  def copy(extra: ParamMap): HardCodedWordCountStage = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType){
      throw new Exception(s"Input type ${field.dataType} did not match StringType")
    }
    schema.add(StructField($(outputCol),IntegerType, false))
  }

  def transform(df: Dataset[_]): DataFrame = {
    val wordcount = udf {in: String => in.split(" ").size}
    df.select(col("*"), wordcount(df.col($(inputCol))).as($(outputCol)))
  }



}
