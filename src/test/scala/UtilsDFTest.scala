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

    val results = ids.transform(UtilsDF.allUniqueIdPairs("id", "id2"))
    assert(results.count() == expected.count())
  }

  test("Get ratings of user pairs") {
    val ratings = Seq(
      (1, 1, 3.1),
      (1, 2, 3.2),
      (2, 2, 3.3),
      (2, 3, 3.4)
    ).toDF("userId", "movieId", "rating")

    val expected = Seq(
      (2, 1, 2, 3.2, 3.3)
    ).toDF("movieId", "userId", "userId2", "rating", "rating2")

    val results = ratings.transform(UtilsDF.toUserPairRatings)
    assert(results.except(expected).count() == 0)
  }

  test("Get matrix for similarity") {
    val pairRatings = Seq(
      (3.1, 4.1),
      (3.2, 4.2)
    ).toDF("x", "y")

    val expected = Seq(
      (3.1, 4.1, 3.1 * 3.1, 4.1 * 4.1, 3.1 * 4.1),
      (3.2, 4.2, 3.2 * 3.2, 4.2 * 4.2, 3.2 * 4.2)
    ).toDF("x", "y", "xx", "yy", "xy")

    val results = pairRatings.transform(UtilsDF.withMatrixFromRatings("x", "y"))

    assert(results.except(expected).count() == 0)
  }

  test("Get similarity from matrix") {
    val matrix = Seq(
      (9, 9, 29, 33, 24, 3)
    ).toDF("x", "y", "xx", "yy", "xy", "n")

    val expected = -0.8660254037844387

    val result = matrix.transform(UtilsDF.withSimilarityFromMatrix)

    assert(result.select("similarity").collect().head.get(0) === expected)
  }

  test("Get predictions from similarities and ratings") {
    val ratingsWithSimilarity = Seq(
      (1,1371,4.0,0.06875238727711523),
      (1,1371,3.0,-0.899228803025897),
      (1,1371,4.0,0.0)
    ).toDF("userId", "movieId", "rating", "similarity")

    val exprected = 2.9172133175929793

    val predictions = ratingsWithSimilarity
      .transform(UtilsDF.toPredictions(
        "userId",
        "movieId",
        "rating",
        "similarity"))

    assert(predictions.select(predictions.col("prediction")).collect().head.get(0).equals(exprected))
  }

  test("Get prediction accuracy") {
    val predictionsAndRatings = Seq(
      (1, 1, 4.0, 4.0),
      (1, 2, 3.0, 3.0),
      (2, 1, 4.0, 2.0),
      (2, 2, 3.0, 1.5),
      (3, 1, 5.0, 0.0),
      (3, 2, 0.0, 5.0),
      (4, 1, 2.0, 4.0),
      (4, 2, 1.5, 3.0)
    ).toDF("userId", "movieId", "prediction", "rating")

    val expected = Seq(
      (1, 1.0),
      (2, .65),
      (3, .0),
      (4, .65)
    ).toDF("userId", "accuracy")

    val result = predictionsAndRatings.transform(UtilsDF.toAccuracy(5.0))

    assert(result.except(expected).count() == 0)
  }
}
