import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class UtilsDFTest extends FunSuite{
  val sc: SparkContext = Utils.initSpark("UtilsDF Test", true)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  test("Get entries of ID") {
    val entries = Seq(
      (1, 1),
      (1, 2),
      (2, 2),
      (2, 3)
    ).toDF("id", "common")
    val expected = Seq(
      (1, 1),
      (1, 2)
    ).toDF("id", "common")

    val result = entries.transform(UtilsDF.ofId(1, "id"))
    assert(expected.except(result).count() == 0)
  }

  test("Get entries related to ID") {
    val entries = Seq(
      (1, 1),
      (1, 2),
      (2, 2),
      (2, 3)
    ).toDF("id", "common")
    val expected = Seq(
      (1, 1),
      (1, 2),
      (2, 2)
    ).toDF("id", "common")

    val result = entries.transform(UtilsDF.relatedToId(1, "id", "common"))
    assert(result.except(expected).count() == 0)
  }
}
