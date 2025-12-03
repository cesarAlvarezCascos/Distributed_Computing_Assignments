package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession


class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date YYYY-MM-DD", required = true)

  val text = opt[Boolean](descr = "use text input", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "use parquet input", required = false, default = Some(false))
  verify()
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Q2Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    val inputPath = args.input()
    val date = args.date()

    val useText = args.text()
    val useParquet = args.parquet()

    if (useText == useParquet) {
      // Only one must be true
      log.warn("Specify exactly one of --text or --parquet")
    }

    val results: Array[(String, Long)] =
      if (useText) {
        runText(sc, inputPath, date)
      } else {
        runParquet(sc, inputPath, date)
      }

    // Output Format
    results.foreach { case (clerk, orderkey) =>
      println(s"($clerk,$orderkey)")
    }
  }

  /**
   * Text Version: join reduce-side with cogroup between lineitem and orders
   */
  def runText(sc: SparkContext, input: String, date: String): Array[(String, Long)] = {
    val lPath = s"$input/lineitem.tbl"
    val oPath = s"$input/orders.tbl"

    val lines = sc.textFile(lPath)
    val orders = sc.textFile(oPath)

    val lineitemByOrder = lines
      .map(_.split("\\|", -1))            
      .filter(fields => fields.length > 10 && fields(10) == date) // Shippings with desired date
      .map { fields =>
        val l_orderkey = fields(0).toLong // orderKey is first column
        (l_orderkey, 1) // FLAG this order because it has shippings in that date
      }

    val ordersByKey = orders
      .map(_.split("\\|", -1))
      .filter(fields => fields.length > 6) // 'CLERK' is 7th column
      .map { fields =>
        val o_orderkey = fields(0).toLong
        val o_clerk = fields(6)      // 'o_clerk' Column
        (o_orderkey, o_clerk)
    }

    // Reduce-side join using cogroup: for each OrderKey
    val joined = lineitemByOrder.cogroup(ordersByKey)  
      // joined: RDD[(Long, (Iterable[Int], Iterable[String]))] where Long is the order Key, 1st Iterable is the count of orders by same Clerk, 2nd Iterable are the Clerks

    val clerkOrder = joined.flatMap { case (orderkey, (liIter, oIter)) =>
      if (liIter.nonEmpty && oIter.nonEmpty) {
        for {
          _  <- liIter
          clerk <- oIter
        } yield (orderkey, clerk)
      } else {
        Seq.empty[(Long, String)]
      }
    }

    // Sort by o_orderkey ascending and take first 20 y tomamos los primeros 20. [web:18]
    val top20 = clerkOrder
      .sortBy({ case (orderkey, _) => orderkey }, ascending = true)
      .take(20)

    // Convert to (clerk, orderkey) 
    top20.map { case (orderkey, clerk) => (clerk, orderkey) }
  }
    

  /**
   * Parquet Version: use SparkSession.read.parquet and then work with RDD
   */
   def runParquet(sc: SparkContext, input: String, date: String): Array[(String, Long)] = {
    val lPath = s"$input/lineitem"
    val oPath = s"$input/orders"
    
    val sparkSession = SparkSession.builder().getOrCreate()
    // DataFrames
    val lineitemDF = sparkSession.read.parquet(lPath)
    val ordersDF = sparkSession.read.parquet(oPath)
    // RDDs (the used .selects are allowed by the statement of the assignment)
    val lineitemRDD = lineitemDF.select("l_orderkey", "l_shipdate").rdd
    val ordersRDD = ordersDF.select("o_orderkey", "o_clerk").rdd

    // Similar to Q1:
    val lineitemByOrder = lineitemRDD
      .map { row =>
        val orderkey = row.getInt(0).toLong
        val shipVal = row.get(1)
        val shipdate = shipVal match {
          case d: java.sql.Date => d.toString
          case s: String => s
          case other => other.toString
        }
        (orderkey, shipdate)
      }
      .filter { case (_, shipdate) => shipdate == date }
      .map { case (orderkey, _) => (orderkey, 1) }

    val ordersByKey = ordersRDD
      .map { row =>
        val orderkey = row.getInt(0).toLong
        val clerk = row.getAs[String](1)
        (orderkey, clerk)
      }

    val joined = lineitemByOrder.cogroup(ordersByKey)

    val clerkOrder = joined.flatMap { case (orderkey, (lIter, oIter)) =>
      if (lIter.nonEmpty && oIter.nonEmpty) {
        for {
          _  <- lIter
          clerk <- oIter
        } yield (orderkey, clerk)
      } else {
        Seq.empty[(Long, String)]
      }
    }

    val top20 = clerkOrder
      .sortBy({ case (orderkey, _) => orderkey }, ascending = true)
      .take(20)

    top20.map { case (orderkey, clerk) => (clerk, orderkey) }
  }
}
