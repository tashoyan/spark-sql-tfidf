package com.github.tashoyan.tfidf

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object Main {
  private val spark = SparkSession.builder()
    .appName("TfIdf")
    .getOrCreate()
  import spark.implicits._

  private val rawTextColumn = "raw_text"
  private val wordsColumn = "words"

  def main(args: Array[String]): Unit = {
    val documents = readDocuments(
      "battle_hymn_of_the_republic.txt",
      "seek_and_destroy.txt"
    )
    documents.show()

    val words = prepareWords(documents)
    words
      .select(wordsColumn)
      .show(false)

    val config = TfIdfConfig(documentColumn = wordsColumn)
    val tfIdf = new TfIdf(config)
    val terms = tfIdf.genTfIdf(words)

    val window = Window.partitionBy(config.docIdColumn)
        .orderBy(col(config.tfIdfColumn).desc)
    val rankColumn = "rank"
    terms
      .withColumn(rankColumn, row_number() over window)
      .where(col(rankColumn) <= 10)
      .select(config.docIdColumn, config.tokenColumn, config.tfIdfColumn)
      .distinct()
      .show(500,false)
  }

  private def readDocuments(names: String*): DataFrame = {
    val docs: Seq[String] = names.map(readDocument)
    docs.toDF(rawTextColumn)
  }

  private def readDocument(name: String): String = {
    val resourceStream = this.getClass
      .getResourceAsStream(name)
    Source.fromInputStream(resourceStream)
      .mkString
  }

  private def prepareWords(rawText: DataFrame): DataFrame = {
    val noPunctColumn = "no_punct"
    val noPunctText = rawText.withColumn(noPunctColumn,
      regexp_replace(col(rawTextColumn), """[\p{Punct}]""", ""))

    val tokenizer: RegexTokenizer = new RegexTokenizer()
      .setInputCol(noPunctColumn)
      .setOutputCol(wordsColumn)
      .setToLowercase(true)
    val words = tokenizer.transform(noPunctText)

    words.where(size(col(wordsColumn)) > 0)
  }
}
