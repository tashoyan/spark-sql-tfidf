package com.github.tashoyan.tfidf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DocumentSearcher(
                        documentIndex: DataFrame,
                        maxTfIdfRank: Int = 5,
                        config: DocumentSearcherConfig = DocumentSearcherConfig()
                      ) {

  def searchDocuments(keyWords: Set[String]): Set[Document] = {
    val foundDocs = documentIndex
      .where(col(config.tokenColumn) isin (keyWords.toSeq: _*))
      .select(config.docNameColumn, config.docPathColumn)
      .distinct()
    foundDocs.collect()
      .map { row =>
        Document(
          row.getAs[String](config.docNameColumn),
          row.getAs[String](config.docPathColumn)
        )
      }
      .toSet
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