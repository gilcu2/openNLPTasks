package openNLP

import java.io.FileInputStream

import opennlp.tools.cmdline.parser.ParserTool
import opennlp.tools.parser.{ParserFactory, ParserModel}

object Parser {

  def main(implicit args: Array[String]): Unit = {

    val sentence = "Tutorialspoint is the largest tutorial library."

    val inputStream = new FileInputStream("models/en-parser-chunking.bin")
    val model = new ParserModel(inputStream)
    val parser = ParserFactory.create(model)

    val topParses = ParserTool.parseLine(sentence, parser, 1)
    topParses.foreach(_.show)
  }

}
