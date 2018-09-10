import DataClasses.Rating
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object Main extends App {
  val sc = Utils.initSpark("Main")
  val sqlContext = new SQLContext(sc)

  //Load files in SparkContext
  val moviesUrl = s"${sys.env("FILE_SERVER_URL")}/movies.csv"
  val ratingsUrl = s"${sys.env("FILE_SERVER_URL")}/ratings.csv"
  if (Try(sc.addFile(moviesUrl)).isFailure ||
      Try(sc.addFile(ratingsUrl)).isFailure) {
    Utils.endProgram("Critical files not added to SparkContext!", sc)
  }

  //Load DataFrames from files
  val moviesTry = Utils.loadFileCSV(sqlContext, SparkFiles.get("movies.csv"))
  val ratingsTry = Utils.loadFileCSV(sqlContext, SparkFiles.get("ratings.csv"))
  if (moviesTry.isFailure) Utils.endProgram("Movies CSV not loaded into DataFrame", sc)
  if (ratingsTry.isFailure) Utils.endProgram("Ratings CSV not loaded into DataFrame", sc)

  val movies = moviesTry.get
  val ratings = ratingsTry.get

  val relatedRatings = ratings
    .transform(UtilsDF.relatedToId(1, "userId", "movieId"))

  val similarity = relatedRatings
      .transform(UtilsDF.toUserPairRatings)
      .transform(UtilsDF.withMatrixFromRatings("rating", "rating2"))
      .withColumn("one", lit(1))
      .groupBy(col("userId"), col("userId2"))
      .agg(sum(col("x")).alias("x"),
        sum(col("y")).alias("y"),
        sum(col("xx")).alias("xx"),
        sum(col("yy")).alias("yy"),
        sum(col("xy")).alias("xy"),
        sum(col("one")).alias("n"))
      .transform(UtilsDF.withSimilarityFromMatrix)

  similarity.show()

  val ratingsWithSimilarity = relatedRatings
    .join(similarity, similarity.col("userId2") === relatedRatings.col("userId"))
    .select(similarity.col("userId"),
      similarity.col("userId2"),
      col("movieId"),
      col("rating"),
      col("similarity"))

  ratingsWithSimilarity.filter(col("movieId") === 1371).show()

  val predictions = ratingsWithSimilarity
      .transform(UtilsDF.toPredictions("userId", "movieId", "rating", "similarity"))
      .toDF("userId", "movieId", "prediction")
  predictions.show()

  val predictionsWithActual =  predictions
    .join(relatedRatings,
      relatedRatings.col("userId") === predictions.col("userId") &&
      relatedRatings.col("movieId") === predictions.col("movieId"))

  predictionsWithActual.show()

  println("Checking accuracy")

  val accuracy = predictionsWithActual
    .select(relatedRatings.col("userId"), relatedRatings.col("movieId"),
      col("rating"), col("prediction"))
    .transform(UtilsDF.toAccuracy(5.0))
  accuracy.show()

  Utils.endProgram("Done", sc)
}
