import DataClasses.{MatrixEntry, Rating, Similarity}
import org.apache.spark.rdd.RDD

import scala.util.{Try, Success, Failure}

object UtilsRDD {

  //To change
  def getSumOfRatingsPerUser(ratings: RDD[Rating])
  : RDD[(Int, Double)] = {
    val sumsOfRatings = ratings
      .map({rating: Rating => (rating.user, rating.score)})
      .reduceByKey((sumRatings, rating) => sumRatings + rating)

    sumsOfRatings
  }

  //To change
  def getUserPredictions(userSimilarities: RDD[Similarity], ratings: RDD[Rating])
  :RDD[Rating] = {

    val totalMovies = ratings.map((rating: Rating) => rating.movie).distinct().count()
    val totalUsers = ratings.map((rating: Rating) => rating.user).distinct().count()

    val predictions = userSimilarities
        .cartesian(ratings)
        .filter({case (similarity: Similarity, rating: Rating) =>
          similarity.user == rating.user
        })
        .map({case (similarity: Similarity, rating: Rating) =>
          ((similarity.user, rating.movie), (rating.score * similarity.score, similarity.score))
        })
        .reduceByKey({case ((ratingSum, similaritySum), (rating, similarity)) =>
          (ratingSum + rating, similaritySum + similarity)
        })
        .map({case ((user, movie), (ratingTot, similarityTot)) =>
          Rating(user, movie, ratingTot / similarityTot)
        })
        .filter((prediction: Rating) => !prediction.score.isNaN)

    predictions
  }

  //To change
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

  //To change
  def getNumberOfRatingsPerUser(ratings: RDD[Rating])
  : RDD[(Int, Int)] = {
    ratings
      .map({rating: Rating => (rating.user, 1)})
      .reduceByKey((totalRatings, one) => totalRatings + one)
  }

  //To change
  def getAvgRatingPerUser(ratings: RDD[Rating])
  : RDD[(Int, Double)] = {

    val sumsOfRatings = getSumOfRatingsPerUser(ratings)
    val amountsOfRatings = UtilsRDD.getNumberOfRatingsPerUser(ratings)

    val userAvgRating = sumsOfRatings
      .join(amountsOfRatings)
      .map({case (userID, (sumOfRatings, numberOfRatings)) => (userID, sumOfRatings / numberOfRatings)})

    userAvgRating
  }

  def getRatingsOfUser(ratings: RDD[Rating], user: Int)
  : RDD[Rating] = ratings.filter((rating: Rating) => rating.user == user)

  def getRatingsRelatedToUser(ratings: RDD[Rating], user: Int)
  : RDD[Rating] = {
    val userRatings = ratings
      .filter((rating: Rating) => rating.user == user)

    val moviesSeen = userRatings.map((rating: Rating) => rating.movie).collect()

    val relatedRatings = ratings
      .filter((rating: Rating) => moviesSeen.contains(rating.movie))

    relatedRatings
  }

  def getUserIds(ratings: RDD[Rating])
  : RDD[Int] = ratings.map(rating => rating.user).distinct()

  //To change
  def getMovieIds(ratings: RDD[Rating])
  : RDD[Int] = ratings.map(rating => rating.movie).distinct()

  def calculatePearsonSimilarity(userOneRatings: RDD[(Int, Double)], userTwoRatings: RDD[(Int, Double)])
  :Try[Double] = {
    val allRatings = userOneRatings.join(userTwoRatings)
    val (x, y, xx, yy, xy, n) = allRatings.map({case (movieID, (x, y)) =>
      (x, y, x * x, y * y, x * y, 1)
    }).reduce({case ((xT, yT, xxT, yyT, xyT, nT), (x, y, xx, yy, xy, n)) =>
      (xT + x, yT + y, xxT + xx, yyT + yy, xyT + xy, nT + n)
    })

    val numerator = xy - (x * y) / n
    val denominator1 = xx - x * x / n
    val denominator2 = yy - y * y / n

    if (denominator1.isNaN || denominator2.isNaN || denominator1 == 0 || denominator2 == 0)
      return Failure(new ArithmeticException("Dividing by 0"))

    Success(numerator / Math.sqrt(denominator1 * denominator2))
  }

  //To change
  def getUserSimilarity(ratings: RDD[Rating])
  : RDD[Similarity] = {
    val userIDs = UtilsRDD.getUserIds(ratings)
    val movieIDs = UtilsRDD.getMovieIds(ratings)

    val matrix = getMatrix(ratings)

    val similarities = matrix
        .reduceByKey({case (acc, entry) =>
          MatrixEntry(
            acc.x + entry.x, acc.y + entry.y, acc.xx + entry.xx, acc.yy + entry.yy, acc.xy + entry.xy, acc.n + entry.n)
        })
        .map({case ((userID1, userID2), entry) =>
          val numerator = entry.xy - (entry.x * entry.y) / entry.n
          val denominator1 = entry.xx - (entry.x * entry.x) / entry.n
          val denominator2 = entry.yy - (entry.y * entry.y) / entry.n

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

    similarities
  }

  //To change
  def getMatrix(ratings: RDD[Rating])
  :RDD[((Int, Int), MatrixEntry)] = {
    ratings
      .cartesian(ratings)
      .filter({case (rating1: Rating, rating2: Rating) =>
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
      .map({case (movieID, ((userID1, rating1), (userID2, rating2))) =>
        ((userID1, userID2),
          MatrixEntry(rating1, rating2, rating1 * rating1, rating2 * rating2, rating1 * rating2))
      })
  }
}
