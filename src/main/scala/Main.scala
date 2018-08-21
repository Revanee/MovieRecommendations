import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

import scala.util.Try

object Main extends App {

  println("Initializing spark...")
  val sparkConf = new SparkConf()
  sparkConf.setAppName("Test")
  sparkConf.setMaster("local[*]")
  sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true") //This is needed to avoid errors on program end
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("ERROR")
  val sqlContext = new SQLContext(sc)
  println("Spark ready")

  val logger = Logger.getLogger("org")

  println("Loading files...")
  sc.addFile("./src/main/resources/ml-latest-small/movies.csv")
  sc.addFile("./src/main/resources/ml-latest-small/ratings.csv")
  val loadMovies: Try[DataFrame] = Utils.loadFileCSV(sc, sqlContext, SparkFiles.get("movies.csv"))
  val loadRatings: Try[DataFrame] = Utils.loadFileCSV(sc, sqlContext, SparkFiles.get("ratings.csv"))
  println("Files loaded")

  println("Preparing data...")
  val movies = loadMovies.get.rdd
  val ratings = loadRatings.get.limit(500).rdd

  val testRatings = sc.parallelize(ratings.takeSample(false, 50, 61345351))
  ratings.subtract(testRatings)

  val userMovieRatingsTest = testRatings
    .map(row => (row(0).toString.toInt, row(1).toString.toInt, row(2).toString.toDouble))

  val movieTitles = movies
    .map(row => (row(0).toString.toInt, row(1).toString))
  val userMovieRatings = ratings
    .map(row => (row(0).toString.toInt, row(1).toString.toInt, row(2).toString.toDouble))

  val totalUsers = userMovieRatings
    .map(t => t._1)
    .distinct()
    .count()
  println("Data ready")

  println("Calculating average user ratings...")
  val userAvgRating = Utils.getAvgUserRatings(userMovieRatings)
  println(s"Calculated ${userAvgRating.count()} average ratings")

  println("Calculating user similarities...")
  val userSimilarity = Utils.getUserSimilarity(userMovieRatings)
  val totalUserSimilarities = userSimilarity.count()
  println(s"Calculated $totalUserSimilarities user similarities")

  println("Calculating predictions")
  val userMoviePredictions = Utils.getUserPredictions(userSimilarity, userMovieRatings)
  println(s"Calculated ${userMoviePredictions.count()} predictions")

  println(s"Accuracy ${Utils.checkPredictionAccuracy(userMoviePredictions, userMovieRatingsTest)}%")

  println("Stopping spark")
  sc.stop()
  println("Done")
}
