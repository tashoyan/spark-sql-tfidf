package com.github.tashoyan.tfidf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfter, FunSuite}

class TfIdfTest extends FunSuite with BeforeAndAfter {

  private val sample: Seq[Seq[String]] = Seq(
    "one flesh one bone one true religion",
    "the only good X is a dead X",
    "one is dead"
  )
    .map(_.split("\\s+").toSeq)

  var spark: SparkSession = _

  before {
    spark = SparkSession.builder()
        .master("local[*]")
        .getOrCreate()
  }

  after {
    spark.stop()
  }

  private def sampleDf = {
    val spark0 = spark
    import spark0.implicits._
    sample.toDF("document")
      .withColumn("another_column", lit(10))
  }

  /*Expected values for TF*IDF are generated by Spark API: CountVectorizer + IDF*/
  test("genTfIdf") {
    val config = TfIdfSparkConfig()
    val tfIdf = new TfIdf(config)
    val result = tfIdf.genTfIdf(sampleDf)
      .cache()
    result
      .orderBy(
        col(config.docIdColumn),
        col(config.tokenColumn),
        col(config.tfIdfColumn).desc
      )
      .show(false)

    val columns = result.columns
    config.productIterator.foreach { mandatoryColumn: Any =>
      assert(columns.contains(mandatoryColumn.asInstanceOf[String]), s"Mandatory column $mandatoryColumn is expected")
    }

    val clusterIds = result.select("another_column")
      .collect()
      .map(_.getAs[Int](0))
    assert(clusterIds.forall(_ == 10), "Custom column another_column must present and must have original value")

    /* Doc 0*/

    val doc0Tokens = result
      .select("token", "tf_idf", "document")
      .collect()
      .map(row => (row.getAs[String](0), row.getAs[Double](1), row.getAs[Seq[String]](2)))
      .filter(_._3 == Seq("one", "flesh", "one", "bone", "one", "true", "religion"))
      .map(v => (v._1, v._2))
    assert(doc0Tokens.length === 7, "Number of tokens in doc 0")

    val doc0One = doc0Tokens.filter { case (token, _) => token == "one" }
    assert(doc0One.length === 3, "Number of tokens 'one' in doc 0")
    assert(doc0One.forall { case (_, tfidf) => tfidf == 0.8630462173553426 }, "TF*IDF for tokens 'one' in doc 0")

    val doc0Flesh = doc0Tokens.filter { case (token, _) => token == "flesh" }
    assert(doc0Flesh.length === 1, "Number of tokens 'flesh' in doc 0")
    assert(doc0Flesh.head._2 === 0.6931471805599453, "TF*IDF for tokens 'flesh' in doc 0")

    /* Doc 1*/

    val doc1Tokens = result
      .select("token", "tf_idf", "document")
      .collect()
      .map(row => (row.getAs[String](0), row.getAs[Double](1), row.getAs[Seq[String]](2)))
      .filter(_._3 == Seq("the", "only", "good", "X", "is", "a", "dead", "X"))
      .map(v => (v._1, v._2))
    assert(doc1Tokens.length === 8, "Number of tokens in doc 1")

    val doc1X = doc1Tokens.filter { case (token, _) => token == "X" }
    assert(doc1X.length === 2, "Number of tokens 'X' in doc 1")
    assert(doc1X.forall { case (_, tfidf) => tfidf == 1.3862943611198906 }, "TF*IDF for tokens 'X' in doc 1")

    val doc1Dead = doc1Tokens.filter { case (token, _) => token == "dead" }
    assert(doc1Dead.length === 1, "Number of tokens 'dead' in doc 1")
    assert(doc1Dead.head._2 === 0.28768207245178085, "TF*IDF for tokens 'dead' in doc 1")

    /* Doc 2*/

    val doc2Tokens = result
      .select("token", "tf_idf", "document")
      .collect()
      .map(row => (row.getAs[String](0), row.getAs[Double](1), row.getAs[Seq[String]](2)))
      .filter(_._3 == Seq("one", "is", "dead"))
      .map(v => (v._1, v._2))
    assert(doc2Tokens.length === 3, "Number of tokens in doc 2")

    val doc2One = doc2Tokens.filter { case (token, _) => token == "one" }
    assert(doc2One.length === 1, "Number of tokens 'one' in doc 2")
    assert(doc2One.head._2 === 0.28768207245178085, "TF*IDF for tokens 'one' in doc 2")

    val doc2Dead = doc2Tokens.filter { case (token, _) => token == "dead" }
    assert(doc2Dead.length === 1, "Number of tokens 'dead' in doc 2")
    assert(doc2Dead.head._2 === 0.28768207245178085, "TF*IDF for tokens 'dead' in doc 2")

  }
}
