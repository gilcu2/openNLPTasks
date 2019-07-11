package sparkTextClassifier

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataPreprocessing {

  def load(path: String)(implicit spark: SparkSession): DataFrame = {
    val lines = spark.read.textFile(path)
    loadFromLines(lines)
  }

  def loadFromLines(data: Dataset[String])(implicit spark: SparkSession): DataFrame =
    spark.read
      .option("inferSchema", "true")
      .json(data)

  def getDesiredFields(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    data.withColumn("reviewTS", concat($"summary", lit(" "), $"reviewText"))
      .drop("helpful")
      .drop("reviewerID")
      .drop("reviewerName")
      .drop("reviewTime")
  }
}
