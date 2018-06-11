package com.github.tashoyan.tfidf

import scala.io.StdIn

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new IllegalArgumentException("Argument is expected: directory with documents database")
    }
    val docsDirPath = args.head

    Console.out.println(s"Indexing documents in $docsDirPath")
    val indexer = new DocumentIndexer(docsDirPath)
    val documentIndex = indexer.buildIndex
      .cache()
    /*Look at the document index*/
    //    documentIndex.show(1000, truncate = false)

    val searcher = new DocumentSearcher(documentIndex)
    while (true) {
      Console.out.println("Enter keywords separated by spaces (CTRL-C for exit):")
      val keyWords = StdIn.readLine()
        .split("""\s+""")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toSet
      if (keyWords.nonEmpty) {
        val documents = searcher.searchDocuments(keyWords)
        val docStr = documents.zipWithIndex
          .map { case (doc, index) => s" $index. ${doc.name}\t${doc.filePath}" }
          .mkString("\n")
        Console.out.println("Found:\n" + docStr)
      }
    }
  }

}
