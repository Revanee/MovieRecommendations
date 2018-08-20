import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.log4j.{Level, Logger}

import scala.util.Try

object Main extends App {

  val sparkConf = new SparkConf()
  sparkConf.setAppName("Test")
  sparkConf.setMaster("local[*]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true") //This is needed to avoid errors on program end

  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val logger = Logger.getLogger("org")

  sc.addFile("./src/main/resources/ml-latest-small/movies.csv")
  sc.addFile("./src/main/resources/ml-latest-small/ratings.csv")

  val loadMovies: Try[DataFrame] = Utils.loadFileCSV(sc, sqlContext, SparkFiles.get("movies.csv"))
  val loadRatings: Try[DataFrame] = Utils.loadFileCSV(sc, sqlContext, SparkFiles.get("ratings.csv"))

  val movies = loadMovies.get.rdd
  val ratings = loadRatings.get.limit(500).rdd

  val movieTitles = movies
    .map(row => (row(0).toString.toInt, row(1).toString))
  val userMovieRating = ratings
    .map(row => (row(0).toString.toInt, row(1).toString.toInt, row(2).toString.toDouble))

  val userAvgRating = Utils.getAvgUserRatings(userMovieRating)

  val userMovieVariance = Utils.getUserMovieVariance(userMovieRating, userAvgRating)

  val userSimilarity = Utils.getUserSimilarity(userMovieRating)

  val userMoviePredictions = Utils.getUserPredictions(userSimilarity, userMovieRating)

  val test = userMoviePredictions
      .map({case (user, movie, score) =>
        (movie, (user, score))
      })
      .join(movieTitles)
      .map({case (movie, ((user, score), title)) =>
        (user, title, score)
      })
      .filter({case (user, title, score) => !score.isNaN})
      .sortBy({case (user, title, score) => score})

  println(test.collect().deep.mkString("\n"))

  sc.stop()
}
