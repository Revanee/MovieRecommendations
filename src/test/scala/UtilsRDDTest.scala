import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import scala.util.{Success, Failure}

class UtilsRDDTest extends FunSuite{
  val sparkConf = new SparkConf()
  sparkConf.setMaster("local[*]")
  sparkConf.setAppName("UtilsRDDTest")
  val sc: SparkContext = SparkContext.getOrCreate(sparkConf)
  sc.setLogLevel("ERROR")

  test("Pearson correlation with normal values") {
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
    assert(similarity.get == 1)
  }
  test("Pearson correlation with normal values 2") {
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
    assert(similarity.get == -0.8660254037844387)
  }
//  test("Pearson correlation with null values") {
//    val u1Ratings = Seq(
//      (1, 4.0),
//      (2, 1.0),
//      (4, 4.0),
//      (5, Double.NaN)
//    )
//
//    val u2Ratings = Seq(
//      (2, 1.0),
//      (4, 4.0),
//      (5, 4.0)
//    )
//
//    val scoreTry = UtilsRDD.calculatePearsonSimilarity(sc.parallelize(u1Ratings), sc.parallelize(u2Ratings))
//    val score = scoreTry match {
//      case Success(value) => value
//      case Failure(exception) => 0.0
//    }
//    assert(score == 1.0)
//  }
}
