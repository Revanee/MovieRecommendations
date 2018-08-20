import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Try

object Utils {
  def loadFileCSV(sc: SparkContext, sqlContext: SQLContext, path: String): Try[DataFrame] = {
    println("Trying to get file at " ++ SparkFiles.get("movies.csv"))
    Try[DataFrame]({
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    }) recover {
      case e: Exception => {
        sc.stop()
        println(s"Error loading file at $path")
        println(e)
        System.exit(0)
        null
      }
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

  def getUserMovieVariance(userMovieRatings: RDD[(Int, Int, Double)], userAvgRatings: RDD[(Int, Double)])
  :RDD[(Int, Int, Double)] = {
    val userMovieVariance = userMovieRatings
      .map({case (userID, movieID, rating) => (userID, (movieID, rating))})
      .join(userAvgRatings)
      .map({case (userID, ((movieID, rating), userAvgRating)) => (userID, movieID, rating - userAvgRating)})

    userMovieVariance
  }

  def getUserSimilarity(userMovieRatings: RDD[(Int, Int, Double)])
  :RDD[(Int, Int, Double)] = {
//  :RDD[(Int, Int, Double, Double)] = {
    val umv = userMovieRatings
        .map({case (userID, movieID, rating) => (userID, (movieID, rating))})
    val matrix = umv
        .cartesian(umv)
        .filter({case ((userID1,(movieID1, rating1)), (userID2,(movieID2, rating2))) =>
          movieID1 == movieID2 && userID1 != userID2
        })
        .map({case ((userID1,(movieID1, rating1)), (userID2,(movieID2, rating2))) =>
          (movieID1, ((userID1, rating1), (userID2, rating2)))
        })
        .filter({case (movieID, ((userID1, rating1), (userID2, rating2))) =>
          !(rating1.isNaN || rating2.isNaN)
        })

    val similarities = matrix
        .map({case (movieID, ((userID1, rating1), (userID2, rating2))) =>
          ((userID1, userID2),
            (rating1, rating2, rating1 * rating1, rating2 * rating2, rating1 * rating2, 1))
        })
        .reduceByKey({case ((sumR1, sumR2, sumA, sumB, sumC, sumN), (rating1, rating2, a, b, c, n)) =>
          (sumR1 + rating1, sumR2 + rating2, sumA + a, sumB + b, sumC + c, sumN + n)
        })
        .map({case ((userID1, userID2), (x, y, xx, yy, xy, n)) =>
          val numerator = xy - (x * y) / n
          val denominator1 = xx - (x * x) / n
          val denominator2 = yy - (y * y) / n

          val correlation = numerator / Math.sqrt(denominator1 * denominator2)

          (userID1, userID2, correlation)
        })
        .filter({case (userID1, userID2, correlation) => !correlation.isNaN})

    similarities
  }

  def getUserPredictions(userSimilarities: RDD[(Int, Int, Double)], ratings: RDD[(Int, Int, Double)])
  :RDD[(Int, Int, Double)] = {
    val similarities = userSimilarities
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
    similarities
  }
}
