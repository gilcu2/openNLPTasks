package openNLP

import java.io.FileInputStream

import opennlp.tools.sentdetect.{SentenceDetectorME, SentenceModel}

object SentenceDetection {

  def main(implicit args: Array[String]): Unit = {

    val text =
      """
        |Hi. How are you? Welcome to Tutorialspoint.
        |We provide free tutorials on various technologies
      """.stripMargin

    val inputStream = new FileInputStream("models/en-sent.bin")
    val model = new SentenceModel(inputStream)
    val detector = new SentenceDetectorME(model)

    val sentences = detector.sentDetect(text)
    sentences.foreach(println)
  }

}
