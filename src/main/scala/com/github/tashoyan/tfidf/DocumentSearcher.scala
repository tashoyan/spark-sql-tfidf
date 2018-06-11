package com.github.tashoyan.tfidf

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Document searcher.
  * Searches by keywords in a data set of documents with ranked words. Words are ranked by TF * IDF.
  *
  * @param rankedWordDocuments Data set of documents with ranked words. Word rank is TF * IDF.
  *                            This data set must have all columns listed in [[DocumentSearcherConfig]].
  * @param maxDocsReturn       A search will return at most this number of documents.
  * @param config              Config object that allows to set custom column in the data set.
  */
class DocumentSearcher(
                        rankedWordDocuments: DataFrame,
                        maxDocsReturn: Int = 5,
                        config: DocumentSearcherConfig = DocumentSearcherConfig()
                      ) {

  /**
    * Searches for documents by keywords.
    *
    * @param keyWords Keywords to search.
    * @return Documents matching keywords in the order of relevance.
    */
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

  /**
    * Gets documents with their ranks for the given set of keywords.
    * Looks for documents in the [[rankedWordDocuments]] data set.
    * For a document, its rank against the user query is a sum of TF * IDF for all user's keywords.
    *
    * @param keyWords      Keywords to look within each document.
    * @param docRankColumn Column in the output data set with calculated document ranks.
    * @return Data set with columns describing documents (id, name, file path) and a column with document ranks.
    */
  protected def getRankedDocuments(keyWords: Set[String], docRankColumn: String): DataFrame = {
    val matchingDocs = rankedWordDocuments
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