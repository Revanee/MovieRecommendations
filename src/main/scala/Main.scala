import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

import scala.util.Success
import scala.util.Failure
import scala.util.Try

import java.io.File

object Main extends App {

  println("Initializing spark...")
  val sparkConf = new SparkConf()
    .setAppName("Movie Recommendations")
    .setMaster("spark://192.168.56.101:7077")
    .set("spark.streaming.stopGracefullyOnShutdown","true") //This is needed to avoid errors on program end
//    .setJars(Seq(
//      "C:\\Users\\Utente\\.ivy2\\cache\\com.databricks\\spark-csv_2.10\\jars\\spark-csv_2.10-1.4.0.jar"
//    ))
  val sc = new SparkContext(sparkConf)
  println(sc.getConf.get("spark.jars").toString)
  sc.setLogLevel("INFO")
  val sqlContext = new SQLContext(sc)
  println("Spark ready")

  val logger = Logger.getLogger("org")

//  sc.addJar("C:\\Users\\Utente\\.ivy2\\cache\\com.databricks\\spark-csv_2.10\\jars\\spark-csv_2.10-1.4.0.jar")

  println("Loading files...")
  sc.addFile("https://raw.githubusercontent.com/Revanee/MovieRecommendations/master/src/main/resources/ml-latest-small/movies.csv")
  sc.addFile("https://raw.githubusercontent.com/Revanee/MovieRecommendations/master/src/main/resources/ml-latest-small/movies.csv")

  println(SparkFiles.getRootDirectory())

  var moviesDF: DataFrame = _
  Utils.loadFileCSV(sc, sqlContext, "hdfs://192.168.56.101:9000/movies.csv") match {
    case Failure(exception) =>
      sc.stop()
      println(exception)
      println("Movies not loaded")
      System.exit(0)
    case Success(value) => moviesDF = value
  }

  var ratingsDF: DataFrame = _
  Utils.loadFileCSV(sc, sqlContext, "hdfs://192.168.56.101:9000/ratings.csv") match {
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
  val ratings = ratingsDF.limit(500).rdd

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
