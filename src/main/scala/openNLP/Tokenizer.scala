package openNLP

import java.io.FileInputStream

import opennlp.tools.tokenize.{TokenizerME, TokenizerModel}

object Tokenizer {

  def main(implicit args: Array[String]): Unit = {

    val text =
      """
        |Hi. How are you? Welcome to Tutorialspoint.
        |We provide free tutorials on various technologies
      """.stripMargin

    val inputStream = new FileInputStream("models/en-token.bin")
    val model = new TokenizerModel(inputStream)
    val tokenizer = new TokenizerME(model)

    val tokens = tokenizer.tokenize(text)
    tokens.foreach(println)
  }

}
