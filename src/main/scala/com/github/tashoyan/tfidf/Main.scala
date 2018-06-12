package com.github.tashoyan.tfidf

import scala.io.StdIn

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new IllegalArgumentException("Argument is expected: directory with documents database")
    }
    val docsDirPath = args.head

    Console.out.println(s"Ranking words in the documents located in $docsDirPath")
    val ranker = new DocumentWordRanker(docsDirPath)
    val rankedWordDocuments = ranker.rankWords
      .cache()
    /*Look at the ranked words for all documents*/
    //    rankedWordDocuments.show(1000, truncate = false)

    val searcher = new DocumentSearcher(rankedWordDocuments)
    while (true) {
      Console.out.println("Enter keywords separated by spaces (CTRL-C for exit):")
      val userInput = StdIn.readLine()
      val keyWords = extractKeyWords(userInput)
      if (keyWords.nonEmpty) {
        val documents = searcher.searchDocuments(keyWords)
        Console.out.println("Found:\n" + toPrettyString(documents))
      }
    }
  }

  private def extractKeyWords(userInput: String): Set[String] =
    Option(userInput).getOrElse("")
      .split("""\s+""")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSet

  private def toPrettyString(documents: Set[Document]): String =
    documents.zipWithIndex
      .map { case (doc, index) => s" ${index + 1}. ${doc.name}\t${doc.filePath}" }
      .mkString("\n")

}
