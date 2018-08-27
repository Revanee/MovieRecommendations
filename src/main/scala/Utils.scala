import java.net.URISyntaxException

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkException, SparkFiles}
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
            throw new Exception(s"$className is not available. Did you assemble a fat jar?")
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

  def getAvgUserRatings(ratings: RDD[(Int, Int, Double)])
  : RDD[(Int, Double)] = {

    val userSumRatings = ratings
      .map({case (userID, movieID, rating) => (userID, rating)})
      .reduceByKey((sumRatings, rating) => sumRatings + rating)

    val userNumberOfRatings = ratings
      .map({case (userID, movieID, rating) => (userID, 1)})
      .reduceByKey((totalRatings, one) => totalRatings + one)

    val userAvgRating = userSumRatings
      .join(userNumberOfRatings)
      .map({case (userID, (sumOfRatings, numberOfRatings)) => (userID, sumOfRatings / numberOfRatings)})

    userAvgRating
  }

  def getUserSimilarity(userMovieRatings: RDD[(Int, Int, Double)])
  :RDD[(Int, Int, Double)] = {
    val umr = userMovieRatings
        .map({case (userID, movieID, rating) => (userID, (movieID, rating))})

    val userIDs = umr
      .map(t => t._1)
      .distinct()

    val movieIDs = umr
      .map(t => t._2._1)
      .distinct()

    val matrix = umr
        .cartesian(umr)
        .filter({case ((userID1,(movieID1, rating1)), (userID2,(movieID2, rating2))) =>
          movieID1 == movieID2 && userID1 != userID2
        })
        .map({case ((userID1,(movieID1, rating1)), (userID2,(movieID2, rating2))) =>
          if (userID1 > userID2) ((userID1,(movieID1, rating1)), (userID2,(movieID2, rating2)))
          else ((userID2,(movieID2, rating2)), (userID1,(movieID1, rating1)))
        })
        .distinct()
        .map({case ((userID1,(movieID1, rating1)), (userID2,(movieID2, rating2))) =>
          (movieID1, ((userID1, rating1), (userID2, rating2)))
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
          (userID1, userID2, score)
        })

    println(s"Similarities calculated ${100.0 * similarities.count() / ((userIDs.count() * userIDs.count() - userIDs.count) / 2.0)}%")
    similarities
  }

  def getUserPredictions(userSimilarities: RDD[(Int, Int, Double)], ratings: RDD[(Int, Int, Double)])
  :RDD[(Int, Int, Double)] = {

    val totalMovies = ratings.map({case (user, movie, rating) => movie}).distinct().count()
    val totalUsers = ratings.map({case (user, movie, rating) => user}).distinct().count()

    val predictions = userSimilarities
        .cartesian(ratings)
        .filter({case ((user1, user2, similarity), (userID, movieID, rating)) => {
          user2 == userID && user1 != user2
        }})
        .map({case ((user1, user2, similarity), (userID, movieID, rating)) => {
          ((user1, movieID), (rating * similarity, similarity))
        }})
        .reduceByKey({case ((ratingSum, similaritySum), (rating, similarity)) =>
          (ratingSum + rating, similaritySum + similarity)
        })
        .map({case ((user, movie), (ratingTot, similarityTot)) =>
          (user, movie, ratingTot / similarityTot)
        })
        .filter({case (user, movie, rating) => !rating.isNaN})

    println(s"Predictions calculated ${100.0 * predictions.count() / (totalUsers * totalMovies)}%")

    predictions
  }

  def checkPredictionAccuracy(predictions: RDD[(Int, Int, Double)], ratings: RDD[(Int, Int, Double)])
  :Double = {
    val predictionsByKey = predictions.map({case (user, movie, score) => ((user, movie), score)})
    val ratingsByKey = ratings.map({case (user, movie, rating) => ((user, movie), rating)})

    val toCompare = predictionsByKey
      .join(ratingsByKey)
      .map({case ((user, movie), (score, rating)) => (score, rating)})

    if (toCompare.count() == 0) throw new Exception("Not enough items to compare accuracy")

    val (totalDeviation, totalRating) = toCompare
      .map({case (score, rating) => (math.abs(score - rating), if (score > rating) score else rating)})
      .reduce({case ((deviationTot, ratingTot), (deviation, rating)) =>
        (deviationTot + deviation, ratingTot + rating)
      })

    val accuracy = 100.0 - 100.0 * (totalDeviation / totalRating)

    accuracy
  }
}
