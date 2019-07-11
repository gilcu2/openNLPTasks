package sparkTextClassifier

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import testUtil.SparkSessionTestWrapper
import testUtil.UtilTest._

class TextClassifierMainTest extends FlatSpec with Matchers with GivenWhenThen with SparkSessionTestWrapper {

  behavior of "TextClassifierMain"

  implicit val sparkSession = spark

  import spark.implicits._

  it should "run the main process" in {

    val lines =
      """
        |{"reviewerID": "AIXZKN4ACSKI", "asin": "1881509818", "reviewerName": "David Briner", "helpful": [0, 0], "reviewText": "This came in on time and I am veru happy with it, I haved used it already and it makes taking out the pins in my glock 32 very easy", "overall": 2.0, "summary": "Woks very good", "unixReviewTime": 1390694400, "reviewTime": "0126, 2014"}
        |{"reviewerID": "A1L5P841VIO02V", "asin": "1881509818", "reviewerName": "Jason A. Kramer", "helpful": [1, 1], "reviewText": "I had a factory Glock tool that I was using for my Glock 26, 27, and 17.  I've since lost it and had needed another.  Since I've used Ghost products prior, and know that they are reliable, I had decided to order this one.Sure enough, this is just as good as a factory tool.", "overall": 1.0, "summary": "Works as well as the factory tool", "unixReviewTime": 1328140800, "reviewTime": "02 2, 2012"}
        |{"reviewerID": "AB2W04NI4OEAD", "asin": "1881509818", "reviewerName": "J. Fernald", "helpful": [2, 2], "reviewText": "If you don't have a 3/32 punch or would like to have one in your Glock bag, this is okay.  The butt end of it is handy for pushing pins back in place.  If you already have a 3/32 punch and don't need another, don't both with this one.", "overall": 2.0, "summary": "It's a punch, that's all.", "unixReviewTime": 1330387200, "reviewTime": "02 28, 2012"}
        |{"reviewerID": "A148SVSWKTJKU6", "asin": "1881509818", "reviewerName": "Jusitn A. Watts \"Maverick9614\"", "helpful": [0, 0], "reviewText": "This works no better than any 3/32 punch you would find at the hardware store. Actually, I think you would be better with a regular punch as it has more to hold on to.", "overall": 4.0, "summary": "It's apunch with a Glock logo.", "unixReviewTime": 1328400000, "reviewTime": "02 5, 2012"}
        |{"reviewerID": "AAAWJ6LW9WMOO", "asin": "1881509818", "reviewerName": "Material Man", "helpful": [0, 0], "reviewText": "I purchased this thinking maybe I need a special tool to easily pop off my base plates for my magazines, but it does the same as a regular punch tool. Glock mags are a pain to get the base plates off.  The tool does not really make a difference.", "overall": 4.0, "summary": "Ok,tool does what a regular punch does.", "unixReviewTime": 1366675200, "reviewTime": "04 23, 2013"}
        |{"reviewerID": "A2XX2A4OJCDNLZ", "asin": "1881509818", "reviewerName": "RatherLiveInKeyWest", "helpful": [0, 0], "reviewText": "Needed this tool to really break down my G22, and it works perfectly for that. No difference from OEM I suspect.However, had an added bonus when realizing that I needed a punch to properly disassemble the bolt carrier in my AR for a thorough cleaning, and this tool worked perfectly to push the carrier's retaining pin out and then back into place after the job was completed. Excellent! It is now in my range bag for safe keeping.", "overall": 5.0, "summary": "Glock punch tool - needed for your Glock and other applications too", "unixReviewTime": 1351814400, "reviewTime": "11 2, 2012"}
        |{"reviewerID": "A283UOBQRUNM4Q", "asin": "1881509818", "reviewerName": "Thomas Dragon", "helpful": [0, 0], "reviewText": "If u don't have it .. Get it. All you need to completely take down your glock. Any model any gen.", "overall": 5.0, "summary": "Great tool", "unixReviewTime": 1402358400, "reviewTime": "06 10, 2014"}
        |{"reviewerID": "AWG3H90WVZ0Z1", "asin": "2094869245", "reviewerName": "Alec Nelson", "helpful": [0, 0], "reviewText": "This light will no doubt capture the attention of nigh t-time drivers. It has three functions for the LED, blinking, strobe (kind of) and solid. The lasers project well and can be set to flash or remain solid. Awesome product. Hopefully it holds up.", "overall": 4.0, "summary": "Bright!", "unixReviewTime": 1377907200, "reviewTime": "08 31, 2013"}
        |{"reviewerID": "A3V52OTJHKIJZX", "asin": "2094869245", "reviewerName": "A. Saenz Jr. \"Bettering self\"", "helpful": [0, 1], "reviewText": "Light and laser torch work well, very bright. Just installed on trike, had it a week, but seems to be well built. Only time will tell.", "overall": 5.0, "summary": "Be seen", "unixReviewTime": 1369612800, "reviewTime": "05 27, 2013"}
        |{"reviewerID": "A3SZBE5F3UQ9EC", "asin": "2094869245", "reviewerName": "ChasRat \"ChasRat\"", "helpful": [0, 0], "reviewText": "Does everything it says it will do. I would like it so that the &#34;lane&#34; markings were a bit brighter on the ground. This does add a bit of safety to riding in the dark, as long as the motorists pay attention.","overall": 5.0, "summary": "Bicycle rear tail light", "unixReviewTime": 1383350400, "reviewTime": "11 2, 2013"}
        |{"reviewerID": "A2HVMUMOKOGCQ9", "asin": "2094869245", "reviewerName": "G. Inman", "helpful": [0, 0], "reviewText": "Very bright.  I would recommend this lite to anyone.  I put one on my wife's bike.  I love this light.", "overall": 4.0, "summary": "Great lite", "unixReviewTime": 1399420800, "reviewTime": "05 7, 2014"}
        |{"reviewerID": "A21AJ9GNCM89MK", "asin": "2094869245", "reviewerName": "Greg", "helpful": [0, 0], "reviewText": "It's cheaply made but does what it is supposed to do. Wish it was USB rechargeable. I don't think it will survive a monsoon but light rain it can handle.", "overall": 3.0, "summary": "It's worth the price they charge.", "unixReviewTime": 1389052800, "reviewTime": "01 7, 2014"}
      """.computeCleanLines

    val lineDS = spark.createDataset(lines)
    TextClassifierMain.process(lineDS, positiveSampling = 0.6)
  }

}
