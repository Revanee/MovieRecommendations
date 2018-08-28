import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

import scala.util.Success
import scala.util.Failure

import DataClasses._

object Main extends App {

  println("Initializing spark...")
  val sparkConf = new SparkConf()
    .setAppName("Movie Recommendations")
    .set("spark.streaming.stopGracefullyOnShutdown","true") //This is needed to avoid errors on program end
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("ERROR")
  val sqlContext = new SQLContext(sc)
  println("Spark ready")

  val logger = Logger.getLogger("org")

  println("Loading files...")
  var moviesDF: DataFrame = _
  Utils.loadFileCSV(sc, sqlContext, SparkFiles.get("movies.csv")) match {
    case Failure(exception) =>
      sc.stop()
      println(exception)
      println("Movies not loaded")
      System.exit(0)
    case Success(value) => moviesDF = value
  }

  var ratingsDF: DataFrame = _
  Utils.loadFileCSV(sc, sqlContext, SparkFiles.get("ratings.csv")) match {
    case Failure(exception) =>
      sc.stop()
      println(exception)
      println("Ratings not loaded")
      System.exit(0)
    case Success(value) => ratingsDF = value
  }
  println("Files loaded")

  println("Preparing data...")
  val movies = moviesDF.rdd
  val allRatings = ratingsDF
    .limit(1000).rdd
    .map(row => Rating(row(0).toString.toInt, row(1).toString.toInt, row(2).toString.toDouble))

  val testRatings = sc.parallelize(allRatings.takeSample(false, 50, 61345351))
  val ratings = allRatings.subtract(testRatings)

  val movieTitles = movies
    .map(row => (row(0).toString.toInt, row(1).toString))

  val totalUsers = ratings
    .map(rating => rating.user)
    .distinct()
    .count()
  println("Data ready")

  println("Calculating average user ratings...")
  val userAvgRating = Utils.getAvgUserRatings(ratings)
  println(s"Calculated ${userAvgRating.count()} average ratings")

  println("Calculating user similarities...")
  val userSimilarities = Utils.getUserSimilarity(ratings)
  val totalUserSimilarities = userSimilarities.count()
  println(s"Calculated $totalUserSimilarities user similarities")

  println("Calculating predictions")
  val predictions = Utils.getUserPredictions(userSimilarities, ratings)
  println(s"Calculated ${predictions.count()} predictions")

  println(s"Accuracy ${Utils.checkPredictionAccuracy(predictions, testRatings)}%")

  println("Stopping spark")
  sc.stop()
  println("Done")
}
