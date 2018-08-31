import java.net.URISyntaxException

import Main.sqlContext
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Try

object Utils {
  def loadFileCSV(sqlContext: SQLContext, fileName: String): Try[DataFrame] = {
    for {
      reader <- Try(sqlContext.read)
      reader <- Try(reader.format("com.databricks.spark.csv"))
      reader <- Try(reader.option("header", "true").option("inferSchema", "true"))
      file <- Try(reader.load(fileName))
    } yield file
  }

  def endProgram(message: String, sc: SparkContext)
  :Unit = {
    println(s"Program ended: $message")
    sc.stop()
    System.exit(0)
  }

  def initSpark(appName: String, local: Boolean = false)
  :SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.streaming.stopGracefullyOnShutdown","true") //This is needed to avoid errors on program end
    if (local) sparkConf.setMaster("local[*]")

    val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
    sc.setLogLevel("ERROR")
    sc
  }
}
