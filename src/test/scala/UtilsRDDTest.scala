import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class UtilsRDDTest extends FunSuite{
  val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]")
  sparkConf.setAppName("UtilsRDDTest")
  val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
  sc.setLogLevel("ERROR")

  test("Pearson correlation test 1") {
    val u1Ratings = Seq(
      (1, 4.0),
      (2, 1.0),
      (4, 4.0)
    )

    val u2Ratings = Seq(
      (2, 1.0),
      (4, 4.0),
      (5, 4.0)
    )

    val similarity = UtilsRDD.calculatePearsonSimilarity(sc.parallelize(u1Ratings), sc.parallelize(u2Ratings))
    assert(similarity == 1)
  }
  test("Pearson correlation test 2") {
    val u1Ratings = Seq(
      (2, 4.0),
      (4, 2.0),
      (5, 3.0)
    )

    val u2Ratings = Seq(
      (2, 1.0),
      (4, 4.0),
      (5, 4.0)
    )

    val similarity = UtilsRDD.calculatePearsonSimilarity(sc.parallelize(u1Ratings), sc.parallelize(u2Ratings))
    assert(similarity == -0.8660254037844387)
  }
}
