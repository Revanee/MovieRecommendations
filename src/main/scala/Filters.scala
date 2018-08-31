import DataClasses.Rating
import org.apache.spark.rdd.RDD
import scala.util.{Try, Success, Failure}

object Filters {
  def ratingsRelatedToUser(ratings: RDD[Rating], user: Int)
  : Try[RDD[Rating]] = {
    val userRatings = ratings.filter((rating: Rating) => rating.user == user)
    val moviesSeen = userRatings.map((rating: Rating) => rating.movie).collect()

    val relatedRatings = ratings.filter((rating: Rating) => moviesSeen.contains(rating.movie))
    Success(relatedRatings)
  }
}
