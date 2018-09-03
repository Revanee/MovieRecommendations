import org.apache.spark.sql.DataFrame
import scala.util.{Try, Success, Failure}

object UtilsDF {
  def getRatingsOfUser (user: Int)(ratings: DataFrame): DataFrame = {
    ratings.filter(s"userId = $user")
  }

  def getRatingsRelatedToUser (user: Int)(ratings: DataFrame): DataFrame = {
    val userRatings = ratings
      .transform(getRatingsOfUser(user))
    val relatedRatings = ratings
      .filter(ratings.col("movieId").isin(userRatings.col("movieId")))
    relatedRatings
  }
}
