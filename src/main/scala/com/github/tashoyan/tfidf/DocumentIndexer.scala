package com.github.tashoyan.tfidf

import java.io.File

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

class DocumentIndexer(
                       docsDirPath: String,
                       config: DocumentIndexerConfig = DocumentIndexerConfig()
                     ) {
  private val spark = SparkSession.builder()
    .appName("TfIdf")
    .getOrCreate()

  import spark.implicits._

  def buildIndex: DataFrame = {
    val documents = readDocuments(docsDirPath)
    val wordsColumn = "words"
    val words = prepareWords(documents, wordsColumn)
    indexImportantWords(words, wordsColumn)
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
    val noAbbrText = rawText.withColumn(noAbbrColumn,
      regexp_replace(col(config.rawTextColumn), """\w+'ll""", ""))

    val noPunctColumn = "no_punct"
    val noPunctText = noAbbrText.withColumn(noPunctColumn,
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

  protected def getStopWords: Array[String] =
    StopWordsRemover.loadDefaultStopWords("english") ++
      Seq("till", "since")

  protected def indexImportantWords(words: DataFrame, wordsColumn: String): DataFrame = {
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

case class DocumentIndexerConfig(
                                  rawTextColumn: String = "raw_text",
                                  docNameColumn: String = "doc_name",
                                  docPathColumn: String = "doc_path"
                                )
