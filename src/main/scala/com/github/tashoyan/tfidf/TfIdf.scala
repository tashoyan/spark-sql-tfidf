package com.github.tashoyan.tfidf

import com.github.tashoyan.tfidf.TfIdfSpark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Utility to calculate TF * IDF metrics for tokens in documents.
  *
  * @param config Configuration object that allows to customize data set column names.
  */
class TfIdf(config: TfIdfConfig = TfIdfConfig()) {

  /**
    * Calculates TF, DF, IDF and TF * IDF metrics for all tokens from the input document set.
    *
    * @param documents Data set with documents.
    *                  The column containing documents is defined by [[TfIdfConfig.documentColumn]] parameter.
    * @return Data set with the following columns (column names are configurable via [[TfIdfConfig]] config object):
    *         <ul>
    *         <li> `document` - input documents
    *         <li> `doc_id` - unique identifier for the each input document
    *         <li> `token` - one token from this document (specified in the `document` column)
    *         <li> `tf` - term frequency of this token within this document
    *         <li> `df` - document frequency of this token across all documents
    *         <li> `idf` - inverse document frequency of this token across all documents
    *         <li> `tf_idf` - TF * IDF of this token for this document
    *         </ul>
    *         Other columns, if any, are retained from the input data set.
    */
  def genTfIdf(documents: DataFrame): DataFrame = {
    val docsWithId = addDocId(documents)
    val unfoldedDocs = unfoldDocs(docsWithId)
    val tokensWithTf = addTf(unfoldedDocs)
    val tokensWithDf = addDf(unfoldedDocs)
    val tokensWithDfIdf = addIdf(tokensWithDf, documents.count())
    val tfIdf = joinTfIdf(tokensWithTf, tokensWithDfIdf)

    tfIdf.join(docsWithId, Seq(config.docIdColumn), "left")
  }

  protected def addDocId(documents: DataFrame): DataFrame =
    documents.withColumn(config.docIdColumn, monotonically_increasing_id())

  protected def unfoldDocs(documents: DataFrame): DataFrame = {
    val columns = documents.columns.map(col) :+
      (explode(col(config.documentColumn)) as config.tokenColumn)
    documents.select(columns: _*)
  }

  protected def addTf(unfoldedDocs: DataFrame): DataFrame =
    unfoldedDocs.groupBy(config.docIdColumn, config.tokenColumn)
      .agg(count(config.documentColumn) as config.tfColumn)

  protected def addDf(unfoldedDocs: DataFrame): DataFrame =
    unfoldedDocs.groupBy(config.tokenColumn)
      .agg(countDistinct(config.docIdColumn) as config.dfColumn)

  protected def addIdf(tokensWithDf: DataFrame, docCount: Long): DataFrame = {
    val calcIdfUdf = udf { df: Long => calcIdf(docCount, df) }
    tokensWithDf.withColumn(config.idfColumn, calcIdfUdf(col(config.dfColumn)))
  }

  protected def joinTfIdf(tokensWithTf: DataFrame, tokensWithDfIdf: DataFrame): DataFrame =
    tokensWithTf
      .join(tokensWithDfIdf, Seq(config.tokenColumn), "left")
      .withColumn(config.tfIdfColumn, col(config.tfColumn) * col(config.idfColumn))

}

case class TfIdfConfig(
    documentColumn: String = "document",
    docIdColumn: String = "doc_id",
    tokenColumn: String = "token",
    tfColumn: String = "tf",
    dfColumn: String = "df",
    idfColumn: String = "idf",
    tfIdfColumn: String = "tf_idf"
)

object TfIdfSpark {

  def calcIdf(docCount: Long, df: Long): Double =
    math.log((docCount.toDouble + 1) / (df.toDouble + 1))

}
