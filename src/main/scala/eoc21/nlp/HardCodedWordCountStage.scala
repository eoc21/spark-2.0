package eoc21.nlp

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructField, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{udf, col}


/**
  * Created by edwardcannon on 06/02/2017.
  */
class HardCodedWordCountStage(override  val uid: String) extends Transformer{

  def this() = this(Identifiable.randomUID(("hardcodedwordcount")))

  def copy(extra: ParamMap): HardCodedWordCountStage = {
    defaultCopy(extra)
  }

  override  def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex("happy_days")
    val field = schema.fields(idx)
    if (field.dataType !=StringType){
      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
    }
    schema.add(StructField("happy_days_counts", IntegerType, false
    ))
  }

  def transform(df: Dataset[_]): DataFrame = {
    val wordcount = udf { in: String => in.split(" ").size }
    df.select(col("*"),
    wordcount(df.col("happy_days")).as("happy_days_counts"))
  }
}
