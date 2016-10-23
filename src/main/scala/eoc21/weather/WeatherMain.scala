package eoc21.weather

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.SparkSession

/**
  * Created by edwardcannon on 23/10/2016.
  * Predict weather index
  */
object WeatherMain {
  //Return type of an object: println(manOf(df1))
  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]): Unit ={
    println("Weather Main")
    val sparkSession = SparkSession.builder.
      master("local[2]")
      .appName("Weather Index Application")
      .getOrCreate()

    val df = sparkSession.read.option("header", "true")
      .option("inferSchema", "true").csv(args(0)) //infer schema on read

    //select columns of interest
    val df1 = df.select("Start_Date","Avg_Temperature","Avg_Rain","Avg_Sun","Weather_index")
    val df2 = df1.withColumnRenamed("Weather_index", "label").drop("Start_Date")
    val assembler = new VectorAssembler()
      .setInputCols(Array("Avg_Temperature", "Avg_Rain", "Avg_Sun"))
      .setOutputCol("features")

    val output = assembler.transform(df2)
    val Array(trainingData, testData) = output.randomSplit(Array(0.7, 0.3))
    output.show()
    // Train a GBT model.
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
    val builtModel = gbt.fit(trainingData)
    val predictions = builtModel.transform(testData)
    predictions.show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
  }
}
