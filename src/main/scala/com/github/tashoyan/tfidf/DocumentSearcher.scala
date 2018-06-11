package com.github.tashoyan.tfidf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DocumentSearcher(
                        documentIndex: DataFrame,
                        maxDocsReturn: Int = 5,
                        config: DocumentSearcherConfig = DocumentSearcherConfig()
                      ) {

  def searchDocuments(keyWords: Set[String]): Set[Document] = {
    val docRankColumn = "doc_rank"
    val rankedDocuments = getRankedDocuments(keyWords, docRankColumn)
    /*Look at ranked documents for these keywords*/
    //    rankedDocuments.show(false)
    val foundDocs = rankedDocuments.orderBy(col(docRankColumn).desc)
      .limit(maxDocsReturn)
      .select(config.docNameColumn, config.docPathColumn)

    foundDocs.collect()
      .map { row =>
        Document(
          row.getAs[String](config.docNameColumn),
          row.getAs[String](config.docPathColumn)
        )
      }
      .toSet
  }

  protected def getRankedDocuments(keyWords: Set[String], docRankColumn: String): DataFrame = {
    val matchingDocs = documentIndex
      .where(col(config.tokenColumn) isin (keyWords.toSeq: _*))
    matchingDocs
      .groupBy(config.docIdColumn, config.docNameColumn, config.docPathColumn)
      .agg(sum(config.tfIdfColumn) as docRankColumn)
      .orderBy(col(docRankColumn).desc)
  }

}

case class DocumentSearcherConfig(
                                   docIdColumn: String = "doc_id",
                                   docNameColumn: String = "doc_name",
                                   docPathColumn: String = "doc_path",
                                   tokenColumn: String = "token",
                                   tfIdfColumn: String = "tf_idf"
                                 )

case class Document(
                     name: String,
                     filePath: String
                   )