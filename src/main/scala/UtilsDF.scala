import org.apache.spark.sql.DataFrame

object UtilsDF {
  def ofId (id: Int, idCol: String)(ratings: DataFrame): DataFrame = {
    ratings.filter(s"$idCol = $id")
  }

  def relatedToId (id: Int, idCol: String, commonItemCol: String)(entries: DataFrame): DataFrame = {
    val entriesOfId = entries
      .transform(ofId(id, idCol))
    val relatedEntries = entries
      .filter(entries.col(commonItemCol).isin(entriesOfId.col(commonItemCol)))
    relatedEntries
  }


}
