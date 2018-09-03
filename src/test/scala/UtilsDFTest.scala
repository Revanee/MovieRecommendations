import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.FunSuite

class UtilsDFTest extends FunSuite{
  val sc: SparkContext = Utils.initSpark("UtilsDF Test", local = true)
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

  test("Get Pairs of IDs") {
    val ids = sc.parallelize(1 to 3).toDF("id")

    val expected = Seq(
      (1, 2),
      (1, 3),
      (2, 1),
      (2, 3),
      (3, 1),
      (3, 2)
    ).toDF("id1", "id2")

    val results = ids.transform(UtilsDF.allUniqueIdPairs)
    assert(results.count() == expected.count())
  }

  test("Get matrix for entries") {
    assert(1 == 1)
  }
}
