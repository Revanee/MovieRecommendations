import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UtilsDF {
  def ofId(id: Int, idCol: String)(ratings: DataFrame): DataFrame = {
    ratings.filter(col(idCol).equalTo(id))
  }

  def relatedToId(id: Int, idCol: String, commonItemCol: String)(entries: DataFrame): DataFrame = {
    val itemsOfId = entries
      .transform(ofId(id, idCol))
      .select(commonItemCol)
      .map(r => r(0))
      .collect
      .toList
    val relatedEntries = entries
      .filter(col(commonItemCol).isin(itemsOfId: _*))
    relatedEntries
  }

  def allUniqueIdPairs(idCol: String, otherIdName: String)(entries: DataFrame): DataFrame = {
    val otherIds = entries.select(col(idCol).alias(otherIdName))
    entries.join(otherIds).filter(col(idCol) !== col(otherIdName))
  }

  def toUserPairRatings(ratings: DataFrame): DataFrame = {
    ratings
      .join(ratings.select(col("userId").as("userId2"),
        col("movieId").as("movieId2"),
        col("rating").as("rating2")))
      .filter(col("userId") !== col("userId2"))
      .filter(col("movieId") === col("movieId2"))
      .groupBy("movieId")
      .agg(
        first("userId").as("userId"),
        first("userId2").as("userId2"),
        first("rating").as("rating"),
        first("rating2").as("rating2"))
  }

  def withMatrixFromRatings(xCol: String, yCol: String)(ratings: DataFrame): DataFrame = {
    ratings
      .withColumn("x", col(xCol))
      .withColumn("y", col(yCol))
      .withColumn("xx", col("x").*(col("x")))
      .withColumn("yy", col("y").*(col("y")))
      .withColumn("xy", col("x").*(col("y")))
  }

  def getSimilarityFromMatrix(matrix: DataFrame): DataFrame = {
    matrix.withColumn("similarity", {
      val numerator = col("xy") - col("x") * col("y") / col("n")
      val denominator1 = col("xx") - col("x") * col("x") / col("n")
      val denominator2 = col("yy") - col("y") * col("y") / col("n")

      numerator / sqrt(denominator1 * denominator2)
    })
  }
}
