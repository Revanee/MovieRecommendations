import java.net.URISyntaxException

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Try

object Utils {
  def loadFileCSV(sc: SparkContext, sqlContext: SQLContext, uri: String): Try[DataFrame] = {
    println(s"Getting file at $uri")
    Try[DataFrame]({
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(uri)
    }) recover {
      case e: SparkException =>
        e.getCause match {
          case e2: ClassNotFoundException =>
            val className = e2.getMessage
            throw new Exception(s"$className is not available")
          case e2: URISyntaxException =>
            throw new Exception(s"Something's wrong with the file path ${e2.getMessage}")
          case e2: Exception =>
            println(e2)
            throw e2
        }
      case e: Exception =>
        println(e)
        throw e
    }
  }

  def endProgram(message: String, sc: SparkContext)
  :Unit = {
    println(s"Program ended: $message")
    sc.stop()
    System.exit(0)
  }

  def initSpark(appName: String)
  :SparkContext = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .set("spark.streaming.stopGracefullyOnShutdown","true") //This is needed to avoid errors on program end
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")

    sc
  }
}
