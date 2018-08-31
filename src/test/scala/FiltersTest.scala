import DataClasses.Rating
import org.scalatest.FunSuite

import scala.util.{Failure, Success, Try}

class FiltersTest extends FunSuite{
  val sc = Utils.initSparkLocal("Filters Test")

  test("Get ratings related to user") {
    val ratings = Seq(
      (1, 1, 5.0),
      (1, 2, 5.0),
      (2, 2, 5.0),
      (2, 3, 5.0),
      (3, 1, 5.0),
      (3, 3, 5.0)
    ).map(value => Rating.tupled(value))
    val relatedRatings = Seq(
      (1, 1, 5.0),
      (1, 2, 5.0),
      (2, 2, 5.0),
      (3, 1, 5.0)
    ).map(value => Rating.tupled(value))

    val relatedRatingsResult = Filters.ratingsRelatedToUser(sc.parallelize(ratings), 1)
    assert(relatedRatingsResult.get.collect.toSeq == relatedRatings)
  }
}
