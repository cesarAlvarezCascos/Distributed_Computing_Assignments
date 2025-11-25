package ca.uwaterloo.cs451.a6


import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession


class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date YYYY-MM-DD", required = true)

  val text = opt[Boolean](descr = "use text input", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "use parquet input", required = false, default = Some(false))
  verify()
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val inputPath = args.input()
    val date = args.date()

    val useText = args.text()
    val useParquet = args.parquet()

    if (useText == useParquet) {
      // Only one must be true
      log.warn("Specify exactly one of --text or --parquet")
    }

    val count: Long =
      if (useText) {
        runText(sc, inputPath, date)
      } else {
        runParquet(sc, inputPath, date)
      }

    // Output Format
    println(s"ANSWER=$count")
  }

  /**
   * Text Version: reads TPC-H-0.1.TXT/lineitem.tbl as a file delimited by '|'.
   * l_shipdate is 11th column (index 10)
   */
  def runText(sc: SparkContext, input: String, date: String): Long = {
    val path = s"$input/lineitem*"
    val lines = sc.textFile(path)

    val count = lines
      .map(_.split("\\|", -1))              // -1 to keep empty fields
      .filter(fields => fields.length > 10 && fields(10) == date)
      .count()
    count
  }

  /**
   * Parquet Version: use SparkSession.read.parquet and then work with RDD
   */
  def runParquet(sc: SparkContext, input: String, date: String): Long = {
    val path = s"$input/lineitem"

    val sparkSession = SparkSession.builder.getOrCreate
    val lineitemDF = sparkSession.read.parquet(path) // Build Dataframe
    val lineitemRDD = lineitemDF.select("l_shipdate").rdd // This select, between the .parquet and the .rdd is allowed

    // l_shipdate in the Parquet may be Date or String -> make it String
    val count = lineitemRDD
      .map { row => val v = row.get(0) // Each row is the shipdate column written into the lineitemRDD previously
        v match {
          case d: java.sql.Date => d.toString
          case s: String => s
          case other => other.toString
        }
      }
      .filter(shipdate => shipdate == date) // Filter
      .count()

    count
  }
}
