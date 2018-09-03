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

  def allUniqueIdPairs(ids: DataFrame): DataFrame = {
    val otherIds = ids.select(col("id").alias("otherId"))
    ids.join(otherIds).filter(col("id") !== col("otherId"))
  }

  def matrixForSimilarity(ratings: DataFrame): Unit = {

  }
}
