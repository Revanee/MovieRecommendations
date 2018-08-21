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
      (1, 1, 3.0),
      (1, 2, 2.0)
    )
    val ratings = sc.parallelize(ratingsSeq)

    assert(Utils.getAvgUserRatings(ratings).collect()(0) === (1, 2.5))
  }

  test("Utils get user similarity") {
    val ratingsSeq = Seq(
      (1, 1, 4.0),
      (1, 2, 1.0),
      (1, 4, 4.0),
      (2, 2, 4.0),
      (2, 4, 2.0),
      (2, 5, 3.0),
      (3, 2, 1.0),
      (3, 4, 4.0),
      (3, 5, 4.0)
    )
    val ratings = sc.parallelize(ratingsSeq)
    val similarities = Utils.getUserSimilarity(ratings)
        .sortBy({case (u1, u2, score) => s"$u1.$u2".toDouble})
        .collect()

    assert(similarities.contains((3, 1, 1.0)))
    assert(similarities.contains((3, 2, -0.8660254037844387)))

    sc.stop()
  }
}
