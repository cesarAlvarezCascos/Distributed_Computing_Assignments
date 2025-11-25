package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date YYYY-MM-DD", required = true)

  val text = opt[Boolean](descr = "use text input", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "use parquet input", required = false, default = Some(false))
  verify()
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Q3Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    val inputPath = args.input()
    val date = args.date()

    val useText = args.text()
    val useParquet = args.parquet()

    if (useText == useParquet) {
        // Only one must be true
      log.warn("Specify exactly one of --text or --parquet")
    }

    val results: Array[(Long, String, String)] =
      if (useText) {
        runText(sc, inputPath, date)
      } else {
        runParquet(sc, inputPath, date)
      }

    // Output Format: (l_orderkey,p_name,s_name)
    results.foreach { case (orderkey, pname, sname) =>
      println(s"($orderkey,$pname,$sname)")
    }
  }

  /**
   * text Version: hash join with broadcast vars between lineitem part and supplier tables
   */
  def runText(sc: SparkContext, input: String, date: String): Array[(Long, String, String)] = {
    val lPath = s"$input/lineitem.tbl"
    val pPath = s"$input/part.tbl"
    val sPath = s"$input/supplier.tbl"

    // part: p_partkey (1st column, index 0), p_name (2nd column, index 1)
    val partMap = sc.textFile(pPath)
      .map(_.split("\\|", -1))
      .filter(fields => fields.length > 1)
      .map { fields =>
        val p_partkey = fields(0).toLong
        val p_name = fields(1)
        (p_partkey, p_name)
      }
      .collect()
      .toMap

    // supplier: s_suppkey (1st column, index 0), s_name (2nd column, index 1)
    val suppMap = sc.textFile(sPath)
      .map(_.split("\\|", -1))
      .filter(fields => fields.length > 1)
      .map { fields =>
        val s_suppkey = fields(0).toLong
        val s_name = fields(1)
        (s_suppkey, s_name)
      }
      .collect()
      .toMap

    // Broadcast Variables
    val partBroadcast = sc.broadcast(partMap)
    val suppBroadcast = sc.broadcast(suppMap)

    // Query CORE
    // lineitem: we need l_orderkey (1), l_partkey (2) and l_suppkey (3). l_shipdate checked as function input variable 'date'
    val matches = sc.textFile(lPath)
      .map(_.split("\\|", -1))
      .filter(fields => fields.length > 10 && fields(10) == date)
      .flatMap { fields =>
        val l_orderkey = fields(0).toLong
        val l_partkey  = fields(1).toLong
        val l_suppkey  = fields(2).toLong

        val partMapValue = partBroadcast.value
        val suppMapValue = suppBroadcast.value

        // Only if part and supplier exist in the part and supplier Maps
        (partMapValue.get(l_partkey), suppMapValue.get(l_suppkey)) match {
          case (Some(pname), Some(sname)) => Some((l_orderkey, pname, sname))
          case _ => None
        }
      }

    // Sort by l_orderkey ascending and take 20
    matches
      .sortBy({ case (orderkey, _, _) => orderkey }, ascending = true)
      .take(20)
  }

  /**
   * Parquet Version: reading with SparkSession.read.parquet and using RDDs
   */
  def runParquet(sc: SparkContext, input: String, date: String): Array[(Long, String, String)] = {
    val lPath = s"$input/lineitem"
    val pPath = s"$input/part"
    val sPath = s"$input/supplier"

    val sparkSession = SparkSession.builder().getOrCreate()
    // DataFrames
    val lineitemDF = sparkSession.read.parquet(lPath)
    val partDF = sparkSession.read.parquet(pPath)
    val suppDF = sparkSession.read.parquet(sPath)
    // RDDs (the used .select's are allowed in the assignment in this case)
    val partRDD = partDF.select("p_partkey", "p_name").rdd
    val suppRDD = suppDF.select("s_suppkey", "s_name").rdd
    val lineitemRDD = lineitemDF.select("l_orderkey", "l_partkey", "l_suppkey", "l_shipdate").rdd

    val partMap = partRDD
      .map { row =>
        val p_partkey = row.get(0) match {  // make key a Long
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        val p_name = row.getString(1)
        (p_partkey, p_name)
      }
      .collect()
      .toMap

    val suppMap = suppRDD
      .map { row =>
        val s_suppkey = row.get(0) match {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        val s_name = row.getString(1)
        (s_suppkey, s_name)
      }
      .collect()
      .toMap

    val partBroadcast = sc.broadcast(partMap)
    val suppBroadcast = sc.broadcast(suppMap)

    val matches = lineitemRDD
      .flatMap { row =>
        val orderkey = row.get(0)
        val partkey = row.get(1)
        val suppkey = row.get(2)
        val shipVal = row.get(3)

        val l_orderkey = orderkey match {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        val l_partkey = partkey match {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        val l_suppkey = suppkey match {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        val shipdate = shipVal match {
          case d: java.sql.Date => d.toString
          case s: String => s
          case other => other.toString
        }

        if (shipdate != date) {
          None
        } else {
          val partMapValue = partBroadcast.value
          val suppMapValue = suppBroadcast.value

          (partMapValue.get(l_partkey), suppMapValue.get(l_suppkey)) match {
            case (Some(pname), Some(sname)) => Some((l_orderkey, pname, sname))
            case _ => None
          }
        }
      }

    matches
      .sortBy({ case (orderkey, _, _) => orderkey }, ascending = true)
      .take(20)
  }
}
