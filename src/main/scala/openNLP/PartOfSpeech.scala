package openNLP

import java.io.FileInputStream

import opennlp.tools.postag.{POSModel, POSSample, POSTaggerME}

object PartOfSpeech {

  def main(implicit args: Array[String]): Unit = {

    val tokens = Array(
      "Mike",
      "and",
      "Smith",
      "are",
      "good",
      "friends"
    )

    val inputStream = new FileInputStream("models/en-pos-maxent.bin")
    val model = new POSModel(inputStream)
    val tagger = new POSTaggerME(model)

    val tags = tagger.tag(tokens)

    val sample = new POSSample(tokens, tags)
    println(sample)
  }

}
