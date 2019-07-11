package sparkTextClassifier

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._

class DataPreprocessingTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "DataPreprocessing"

  implicit val sparkSession = spark

  import spark.implicits._

  it should "return a dataframe with the desired fields" in {

    val lines =
      """
        |{"reviewerID": "AIXZKN4ACSKI", "asin": "1881509818", "reviewerName": "David Briner", "helpful": [0, 0], "reviewText": "This came in on time and I am veru happy with it, I haved used it already and it makes taking out the pins in my glock 32 very easy", "overall": 5.0, "summary": "Woks very good", "unixReviewTime": 1390694400, "reviewTime": "0126, 2014"}
        |{"reviewerID": "A1L5P841VIO02V", "asin": "1881509818", "reviewerName": "Jason A. Kramer", "helpful": [1, 1], "reviewText": "I had a factory Glock tool that I was using for my Glock 26, 27, and 17.  I've since lost it and had needed another.  Since I've used Ghost products prior, and know that they are reliable, I had decided to order this one.Sure enough, this is just as good as a factory tool.", "overall": 5.0, "summary": "Works as well as the factory tool", "unixReviewTime": 1328140800, "reviewTime": "02 2, 2012"}
        |{"reviewerID": "AB2W04NI4OEAD", "asin": "1881509818", "reviewerName": "J. Fernald", "helpful": [2, 2], "reviewText": "If you don't have a 3/32 punch or would like to have one in your Glock bag, this is okay.  The butt end of it is handy for pushing pins back in place.  If you already have a 3/32 punch and don't need another, don't both with this one.", "overall": 4.0, "summary": "It's a punch, that's all.", "unixReviewTime": 1330387200, "reviewTime": "02 28, 2012"}
        |{"reviewerID": "A148SVSWKTJKU6", "asin": "1881509818", "reviewerName": "Jusitn A. Watts \"Maverick9614\"", "helpful": [0, 0], "reviewText": "This works no better than any 3/32 punch you would find at the hardware store. Actually, I think you would be better with a regular punch as it has more to hold on to.", "overall": 4.0, "summary": "It's apunch with a Glock logo.", "unixReviewTime": 1328400000, "reviewTime": "02 5, 2012"}
      """.computeCleanLines

    val lineDS = spark.createDataset(lines)
    val originalFields = DataPreprocessing.loadFromLines(lineDS)
    val desiredFields = DataPreprocessing.getDesiredFields(originalFields)

    desiredFields.printSchema
    desiredFields.show

    val filtered = desiredFields.filter("overall !=3")

    val bucketized = Statistics.bucketize(filtered, "overall", "label")
    bucketized.groupBy("overall", "label").count.show


  }

}
