import org.apache.spark.SparkFiles
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.{Failure, Success, Try}

object Main extends App {
  val sc = Utils.initSpark("Main")
  val sqlContext = new SQLContext(sc)

  val moviesUrl = s"${sys.env("FILE_SERVER_URL")}/movies.csv"
  val ratingsUrl = s"${sys.env("FILE_SERVER_URL")}/ratings.csv"
  if (Try(sc.addFile(moviesUrl)).isFailure ||
      Try(sc.addFile(ratingsUrl)).isFailure) {
    Utils.endProgram("Critical files not added to SparkContext!", sc)
  }

  val movies = Utils.loadFileCSV(sqlContext, SparkFiles.get("movies.csv"))
  val ratings = Utils.loadFileCSV(sqlContext, SparkFiles.get("ratings.csv"))
  if (movies.isFailure || ratings.isFailure) {
    Utils.endProgram("CSV files not loaded into DataFrames", sc)
  }

  println(movies)
  println(ratings)

  Utils.endProgram("Done", sc)
}
