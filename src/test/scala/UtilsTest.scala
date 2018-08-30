import DataClasses.{Rating, Similarity}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class UtilsTest extends FunSuite {
  val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]")
  sparkConf.setAppName("Test")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("ERROR")

  test("Utils get average user ratings") {
    val ratingsSeq = Seq(
      Rating(1, 1, 3.0),
      Rating(1, 2, 2.0)
    )
    val ratings = sc.parallelize(ratingsSeq)

    assert(UtilsRDD.getAvgRatingPerUser(ratings).collect()(0) === (1, 2.5))
  }

  test("Utils get user similarity") {
    val ratingsSeq: Seq[Rating] = Seq(
      (1, 1, 4.0),
      (1, 2, 1.0),
      (1, 4, 4.0),
      (2, 2, 4.0),
      (2, 4, 2.0),
      (2, 5, 3.0),
      (3, 2, 1.0),
      (3, 4, 4.0),
      (3, 5, 4.0)
    ).map(value => Rating(value._1, value._2, value._3))
    val ratings = sc.parallelize(ratingsSeq)
    val similarities = UtilsRDD.getUserSimilarity(ratings)
        .sortBy((similarity: Similarity) => s"${similarity.user}.${similarity.otherUser}".toDouble)
        .collect()

    assert(similarities.contains(Similarity(3, 1, 1.0)))
    assert(similarities.contains(Similarity(3, 2, -0.8660254037844387)))
  }

  test("Utils get accuracy") {
    val ratings = Seq(
      (1, 1, 2.5),
      (1, 2, 4.0),
      (1, 3, 2.0),
      (2, 1, 1.5),
      (2, 2, 1.0),
      (2, 3, 0.5)
    ).map(value => Rating(value._1, value._2, value._3))

    val predictions = Seq(
      (1, 1, 5.0),
      (1, 2, 2.0),
      (2, 1, 3.0),
      (2, 2, 2.0)
    ).map(value => Rating(value._1, value._2, value._3))

    val accuracy = UtilsRDD.checkPredictionAccuracy(sc.parallelize(predictions), sc.parallelize(ratings))

    assert(accuracy === 50.0)
  }

  test("Utils get ratings related to user") {
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

    val relatedRatingsResult = UtilsRDD.getRatingsRelatedToUser(1, sc.parallelize(ratings))
    assert(relatedRatingsResult.collect().toSeq == relatedRatings)
  }
}
