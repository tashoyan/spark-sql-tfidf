package com.github.tashoyan.tfidf

import java.io.File

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

/**
  * Assigns weights to words in documents.
  * Weights are actually TF * IDF.
  *
  * @param docsDirPath Path to the directory with text documents.
  * @param config      Config object that allows to set custom column in the data set.
  */
class WeightCalculator(
                          docsDirPath: String,
                          config: WeightCalculatorConfig = WeightCalculatorConfig()
                        ) {
  private val spark = SparkSession.builder()
    .appName("TfIdf")
    .getOrCreate()

  import spark.implicits._

  /**
    * Assigns weights to all words in all documents obtained from the [[docsDirPath]] directory.
    *
    * @return Data set with columns describing documents (id, name, file path)
    *         and word weights (word, its weight).
    *         Each word is listed at most once for each document.
    */
  def calcWordWeights: DataFrame = {
    val documents = readDocuments(docsDirPath)
    val wordsColumn = "words"
    val words = prepareWords(documents, wordsColumn)
    calcWordWeights(words, wordsColumn)
  }

  protected def readDocuments(docsDirPath: String): DataFrame = {
    val docsDir = new File(docsDirPath)
    if (!docsDir.isDirectory) {
      throw new IllegalArgumentException(s"Not a directory: $docsDirPath")
    }

    val docFiles = docsDir.listFiles()
      .filter(_.isFile)
    if (docFiles.isEmpty) {
      throw new IllegalArgumentException(s"None files found in the directory: $docsDirPath")
    }

    docFiles.map { docFile =>
      (docFile.getName, docFile.getAbsolutePath, Source.fromFile(docFile).mkString)
    }
      .toSeq
      .toDF(config.docNameColumn, config.docPathColumn, config.rawTextColumn)
  }

  protected def prepareWords(rawText: DataFrame, wordsColumn: String): DataFrame = {
    val noAbbrColumn = "no_abbr"
    val noAbbrText = removeAbbreviations(rawText, noAbbrColumn)

    val noPunctColumn = "no_punct"
    val noPunctText = noAbbrText
      .withColumn(noPunctColumn,
        regexp_replace(col(noAbbrColumn), """[\p{Punct}]""", ""))

    val rawWordsColumn = "raw_words"
    val tokenizer: RegexTokenizer = new RegexTokenizer()
      .setInputCol(noPunctColumn)
      .setOutputCol(rawWordsColumn)
      .setToLowercase(true)
    val rawWords = tokenizer.transform(noPunctText)
      .where(size(col(rawWordsColumn)) > 0)

    val stopWordsRemover = new StopWordsRemover()
      .setInputCol(rawWordsColumn)
      .setOutputCol(wordsColumn)
      .setStopWords(getStopWords)
    stopWordsRemover.transform(rawWords)
  }

  protected def removeAbbreviations(rawText: DataFrame, noAbbrColumn: String): DataFrame = {
    val zeroDf = rawText.withColumn(noAbbrColumn, col(config.rawTextColumn))
    getAbbreviations
      .foldLeft(zeroDf) { (df, abbr) =>
        df.withColumn(noAbbrColumn, regexp_replace(col(noAbbrColumn), abbr, ""))
      }
  }

  protected def getAbbreviations: Seq[String] =
  //TODO Add more if needed
    Seq(
      """(?i)\w+'ll""",
      """(?i)\w+'re""",
      """(?i)\w+'ve""",
      """(?i)\w+'s""",
      """(?i)i'm""",
      """(?i)\w+'t"""
    )

  protected def getStopWords: Array[String] =
    StopWordsRemover.loadDefaultStopWords("english") ++
      Seq("till", "since")

  protected def calcWordWeights(words: DataFrame, wordsColumn: String): DataFrame = {
    val tfIdfConfig = TfIdfConfig(documentColumn = wordsColumn)
    val tfIdf = new TfIdf(tfIdfConfig)
    val tfIdfWords = tfIdf.genTfIdf(words)

    tfIdfWords
      .select(
        tfIdfConfig.docIdColumn,
        config.docNameColumn,
        config.docPathColumn,
        tfIdfConfig.tokenColumn,
        tfIdfConfig.tfIdfColumn
      )
      .orderBy(col(tfIdfConfig.docIdColumn), col(tfIdfConfig.tfIdfColumn).desc)
  }

}

case class WeightCalculatorConfig(
                         rawTextColumn: String = "raw_text",
                         docNameColumn: String = "doc_name",
                         docPathColumn: String = "doc_path"
                       )
