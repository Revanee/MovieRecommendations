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

  val movies = loadMovies.get.limit(100).rdd
  val ratings = loadRatings.get.limit(500).rdd

  val userMovieRating = ratings
    .map(row => (row(0).toString.toInt, row(1).toString.toInt, row(2).toString.toDouble))

  val userAvgRating = Utils.getAvgUserRatings(userMovieRating)

  val userMovieVariance = userMovieRating
    .map({case (userID, movieID, rating) => (userID, (movieID, rating))})
    .join(userAvgRating)
    .map({case (userID, ((movieID, rating), avgRating)) => (userID, movieID, rating - avgRating)})

  val userIds = userMovieRating
    .map({case (userID, movieID, rating) => userID})
    .distinct()

  val userUserPairs = userIds
    .cartesian(userIds)
    .filter({case (userID, user2ID) => userID != user2ID})

  val userUserMovieVarVarSum = userUserPairs
    .cartesian(userMovieVariance)
    .filter({case ((user1ID, user2ID), (userID, movieID, variance)) => user1ID == userID || user2ID == userID})
    .map({case ((user1ID, user2ID), (userID, movieID, variance)) =>
      ((user1ID, user2ID), (movieID, if (user1ID == userID) variance else 0.0, if (user2ID == userID) variance else 0.0))
    })
    .map({case ((user1ID, user2ID), (movieID, user1Var, user2Var)) => ((user1ID, user2ID, movieID), (user1Var, user2Var))})
    .reduceByKey({case ((user1VarTot, user2VarTot), (user1Var, user2Var)) => (user1VarTot + user1Var, user2VarTot + user2Var)})
    .map({case ((user1ID, user2ID, movieID), (user1Var, user2Var)) => ((user1ID, user2ID), user1Var * user2Var)})
    .filter({case ((user1ID, user2ID), variance) => variance != 0})
    .map({case ((user1ID, user2ID), variance) => if (user1ID > user2ID)
      ((user1ID, user2ID), variance) else
      ((user2ID, user1ID), variance)
    })
    .distinct()

  println(userUserMovieVarVarSum.collect().deep.mkString("\n"))

  sc.stop()
}
