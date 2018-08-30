import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD

import scala.util.Success
import scala.util.Failure

import DataClasses._

object Main extends App {
  println("Initializing spark...")
  val sc = Utils.initSpark("Movie Recommendations")
  val sqlContext = new SQLContext(sc)
  println("Spark ready")

  println("Loading files...")
  val moviesDF = Utils.loadFileCSV(sc, sqlContext, SparkFiles.get("movies.csv")) match {
    case Failure(exception) =>
      Utils.endProgram("Movies not loaded", sc)
      throw exception
    case Success(value) => value
  }
  val ratingsDF = Utils.loadFileCSV(sc, sqlContext, SparkFiles.get("ratings.csv")) match {
    case Failure(exception) =>
      Utils.endProgram("Ratings not loaded", sc)
      throw exception
    case Success(value) => value
  }
  println("Files loaded")

  println("Preparing data...")
  val movies = moviesDF.rdd
  val allRatings: RDD[Rating] = ratingsDF
    .limit(10000).rdd
    .map(row => Rating(row(0).toString.toInt, row(1).toString.toInt, row(2).toString.toDouble))

  val userToAnalyze = 1
  val ratingsRelatedToUser = Utils.getRatingsRelatedToUser(userToAnalyze, allRatings)

  val ratingsForTesting: RDD[Rating] = sc.parallelize(
    ratingsRelatedToUser.takeSample(withReplacement = false, (ratingsRelatedToUser.count() / 5).toInt, 61345351)
  )
  val ratingsToAnalyze: RDD[Rating] = ratingsRelatedToUser.subtract(ratingsForTesting)
  println(s"Test ratings: ${ratingsForTesting.count()}")
  println(s"Ratings related to user: ${ratingsRelatedToUser.count()}")
  println(s"Final ratings: ${ratingsToAnalyze.count()}")
  println("Data ready")



  println("Stopping spark")
  Utils.endProgram("Success", sc)
}
