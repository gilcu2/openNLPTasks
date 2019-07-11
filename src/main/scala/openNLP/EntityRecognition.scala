package openNLP

import java.io.FileInputStream

import opennlp.tools.namefind.{NameFinderME, TokenNameFinderModel}

object EntityRecognition {

  def main(implicit args: Array[String]): Unit = {

    val tokens = Array(
      "Mike",
      "and",
      "Smith",
      "are",
      "good",
      "friends"
    )

    val inputStream = new FileInputStream("models/en-ner-person.bin")
    val model = new TokenNameFinderModel(inputStream)
    val nameFinder = new NameFinderME(model)

    val names = nameFinder.find(tokens)
    names.foreach(println)
  }

}
