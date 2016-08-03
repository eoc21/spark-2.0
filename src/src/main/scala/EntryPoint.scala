import org.apache.spark.sql.SparkSession

/**
  * Created by edwardcannon on 03/08/2016.
  */
object EntryPoint {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local[2]")
      .appName("spark session example")
      .getOrCreate()

    val df = sparkSession.read.option("header", "true").
      csv("/Users/edwardcannon/Documents/unilever-2016/campaignAnalysis/potnoodlecombined.csv")
    println(df.head())
  }
}
