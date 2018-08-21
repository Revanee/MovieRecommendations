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
  val ratings = sc.parallelize(loadRatings.get.rdd.sortBy(Row => Row(1).toString.toInt).take(500))

  val testRatings = sc.parallelize(ratings.take(50))
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

  val userAvgRating = Utils.getAvgUserRatings(userMovieRatings)

  val userSimilarity = Utils.getUserSimilarity(userMovieRatings)
  val totalUserSimilarities = userSimilarity.count()

  val userMoviePredictions = Utils.getUserPredictions(userSimilarity, userMovieRatings)

  val test = userMovieRatingsTest
      .map({case (user, movie, rating) => ((user, movie), rating)})

  val predictions = userMoviePredictions
      .map({case (user, movie, score) => ((user, movie), score)})

  val testResults = test
      .join(predictions)
      .map({case ((user, movie), (rating, score)) =>
        (rating, score, 5.0)
      })
      .filter({case (rating, score, max) =>
        !rating.isNaN && !score.isNaN && !max.isNaN
      })
      .reduce({case ((ratingTot, scoreTot, maxTot), (rating, score, max)) =>
        (ratingTot + rating, scoreTot + score, maxTot + max)
      })

  println(s"Prediction accuracy: ${100.0 - Math.abs(testResults._1 - testResults._2) / testResults._3}%")
//  println(userMoviePredictions.collect().deep.mkString("\n"))
//  userMoviePredictions.collect()
  sc.stop()
}
