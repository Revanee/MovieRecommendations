import java.net.URISyntaxException

import DataClasses.{Rating, Similarity}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkException}
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
          case e2: ClassNotFoundException => {
            val className = e2.getMessage
            throw new Exception(s"$className is not available")
          }
          case e2: URISyntaxException => {
            throw new Exception(s"Something's wrong with the file path ${e2.getMessage}")
          }
          case e2: Exception =>
            println(e2)
            throw e2
        }
      case e: Exception =>
        println(e)
        throw e
    }
  }

  def getAvgUserRatings(ratings: RDD[Rating])
  : RDD[(Int, Double)] = {

    val userSumRatings = ratings
      .map({rating: Rating => (rating.user, rating.score)})
      .reduceByKey((sumRatings, rating) => sumRatings + rating)

    val userNumberOfRatings = ratings
      .map({rating: Rating => (rating.user, 1)})
      .reduceByKey((totalRatings, one) => totalRatings + one)

    val userAvgRating = userSumRatings
      .join(userNumberOfRatings)
      .map({case (userID, (sumOfRatings, numberOfRatings)) => (userID, sumOfRatings / numberOfRatings)})

    userAvgRating
  }

  def getUserSimilarity(ratings: RDD[Rating])
  :RDD[Similarity] = {
    val userIDs = ratings
      .map((rating: Rating) => rating.user)
      .distinct()

    val movieIDs = ratings
      .map((rating: Rating) => rating.movie)
      .distinct()

    val matrix = ratings
        .cartesian(ratings)
        .filter({case (rating1: Rating, rating2: Rating) =>
          //movieID1 == movieID2 && userID1 != userID2
          rating1.movie == rating2.movie && rating1.user != rating2.user
        })
        .map({case (rating1: Rating, rating2: Rating) =>
          if (rating1.user > rating2.user) (rating1, rating2)
          else (rating2, rating1)
        })
        .distinct()
        .map({case (rating1: Rating, rating2: Rating) =>
          (rating1.movie, ((rating1.user, rating1.score), (rating2.user, rating2.score)))
        })

    println(s"Matrix filled ${(100.0 * matrix.count()) / (userIDs.count() * userIDs.count() * movieIDs.count())}%")

    val similarities = matrix
        .map({case (movieID, ((userID1, rating1), (userID2, rating2))) =>
          ((userID1, userID2),
            (rating1, rating2, rating1 * rating1, rating2 * rating2, rating1 * rating2, 1))
        })
        .reduceByKey({case ((xTot, yTot, xxTot, yyTot, xyTot, nTot), (x, y, xx, yy, xy, n)) =>
          (xTot + x, yTot + y, xxTot + xx, yyTot + yy, xyTot + xy, nTot + n)
        })
        .map({case ((userID1, userID2), (x, y, xx, yy, xy, n)) =>
          val numerator = xy - (x * y) / n
          val denominator1 = xx - (x * x) / n
          val denominator2 = yy - (y * y) / n

          val correlation = numerator / Math.sqrt(denominator1 * denominator2)

          (userID1, userID2, correlation)
        })
        .filter({case (userID1, userID2, correlation) => !correlation.isNaN})
        .map({case (userID1, userID2, score) =>
          if (userID1 > userID2) (userID1, (userID2, score))
          else (userID2, (userID1, score))
        })
        .distinct()
        .map({case (userID1, (userID2, score)) =>
          Similarity(userID1, userID2, score)
        })

    println(s"Similarities calculated ${100.0 * similarities.count() / ((userIDs.count() * userIDs.count() - userIDs.count) / 2.0)}%")
    similarities
  }

  def getUserPredictions(userSimilarities: RDD[Similarity], ratings: RDD[Rating])
  :RDD[Rating] = {

    val totalMovies = ratings.map((rating: Rating) => rating.movie).distinct().count()
    val totalUsers = ratings.map((rating: Rating) => rating.user).distinct().count()

    val predictions = userSimilarities
        .cartesian(ratings)
        .filter({case (similarity: Similarity, rating: Rating) => {
          similarity.user == rating.user
        }})
        .map({case (similarity: Similarity, rating: Rating) => {
          ((similarity.user, rating.movie), (rating.score * similarity.score, similarity.score))
        }})
        .reduceByKey({case ((ratingSum, similaritySum), (rating, similarity)) =>
          (ratingSum + rating, similaritySum + similarity)
        })
        .map({case ((user, movie), (ratingTot, similarityTot)) =>
          Rating(user, movie, ratingTot / similarityTot)
        })
        .filter((prediction: Rating) => !prediction.score.isNaN)

    println(s"Predictions calculated ${100.0 * predictions.count() / (totalUsers * totalMovies)}%")

    predictions
  }

  def checkPredictionAccuracy(predictions: RDD[Rating], ratings: RDD[Rating])
  :Double = {
    val predictionsByKey = predictions
      .map((prediction: Rating) => ((prediction.user, prediction.movie), prediction.score))
    val ratingsByKey = ratings.map((rating: Rating) => ((rating.user, rating.movie), rating))

    val toCompare = predictionsByKey
      .join(ratingsByKey)
      .map({case ((user, movie), (score, rating)) => (score, rating)})

    if (toCompare.count() == 0) throw new Exception("Not enough items to compare accuracy")

    val (totalDeviation, totalRating) = toCompare
      .map({case (score, rating) => (math.abs(score - rating.score), if (score > rating.score) score else rating.score)})
      .reduce({case ((deviationTot, ratingTot), (deviation, rating)) =>
        (deviationTot + deviation, ratingTot + rating)
      })

    val accuracy = 100.0 - 100.0 * (totalDeviation / totalRating)

    accuracy
  }
}
