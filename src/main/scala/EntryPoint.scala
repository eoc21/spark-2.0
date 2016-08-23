import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.functions._

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
      csv(args(0))
    println(df.show())
    df.printSchema()
    println("Breakdown by campaign")
    println(df.groupBy("campaign id").count().show())
    //create temporary view on the table
    df.createOrReplaceTempView("potnoodle")
    val df1 = sparkSession.sql("SELECT * FROM potnoodle WHERE CPF > 30")
    val wikipage = new WikipediaWebScraper()
    //Test webpage extraction & extraction of links
    val data = wikipage.extractWebPage("https://en.wikipedia.org/wiki/Panda_cow")
    val links = wikipage.extractLinks()
  }
}
