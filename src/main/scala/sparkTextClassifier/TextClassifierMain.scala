package sparkTextClassifier

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import interfaces.Spark
import org.apache.spark.SparkConf
import interfaces.Time._
import org.apache.spark.sql.{Dataset, SparkSession}

object TextClassifierMain extends LazyLogging {

  def main(implicit args: Array[String]): Unit = {

    val beginTime = getCurrentTime
    logger.info(s"Begin: $beginTime")
    logger.info(s"Arguments: $args")

    implicit val conf = ConfigFactory.load
    val sparkConf = new SparkConf().setAppName("Text Classifier")
    implicit val spark = Spark.sparkSession(sparkConf)

    val lines = spark.read.textFile("data/reviews_Sports_and_Outdoors_5.json")
    process(lines)

  }

  def process(lines: Dataset[String], positiveSampling: Double = 0.1)(implicit spark: SparkSession): Unit = {
    val originalFields = DataPreprocessing.loadFromLines(lines)
    val selectedFields = DataPreprocessing.getDesiredFields(originalFields)

    selectedFields.printSchema
    selectedFields.show

    val filtered = selectedFields.filter("overall !=3")

    val bucketized = Statistics.bucketize(filtered, "overall", "label")
    bucketized.groupBy("overall", "label").count.show

    val fractions = Map(1.0 -> positiveSampling, 0.0 -> 1.0)
    val sampled = MachineLearning.sample(bucketized, "label", fractions)
    sampled.groupBy("label").count.show

  }
}
