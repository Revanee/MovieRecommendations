import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.util.Try

object Utils {
  def loadFileCSV(sc: SparkContext, sqlContext: SQLContext, path: String): Try[DataFrame] = {
    println("Trying to get file at " ++ SparkFiles.get("movies.csv"))
    Try[DataFrame]({
      sqlContext.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(path)
    }) recover {
      case e: Exception => {
        sc.stop()
        println(s"Error loading file at $path")
        println(e)
        System.exit(0)
        null
      }
    }
  }

  def getAvgUserRatings(ratings: RDD[(Int, Int, Double)]): RDD[(Int, Double)] = {

    val userSumRatings = ratings
      .map({case (userID, movieID, rating) => (userID, rating)})
      .reduceByKey((sumRatings, rating) => sumRatings + rating)

    val userNumberOfRatings = ratings
      .map({case (userID, movieID, rating) => (userID, 1)})
      .reduceByKey((totalRatings, one) => totalRatings + one)

    val userAvgRating = userSumRatings
      .join(userNumberOfRatings)
      .map({case (userID, (sumOfRatings, numberOfRatings)) => (userID, sumOfRatings / numberOfRatings)})

    userAvgRating
  }
}
