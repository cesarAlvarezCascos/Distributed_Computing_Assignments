package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](descr = "input path", required = true)

  val text = opt[Boolean](descr = "use text input", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "use parquet input", required = false, default = Some(false))
  verify()
}

object Q5 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Q5Conf(argv)

    log.info("Input: " + args.input())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    val inputPath = args.input()

    val useText = args.text()
    val useParquet = args.parquet()

    if (useText == useParquet) {
      // Only one must be true
      log.warn("Specify exactly one of --text or --parquet")
    }

  
    val results: Array[(Int, String, String, Long)] =
      if (useText) {
        runText(sc, inputPath)
      } else {
        runParquet(sc, inputPath)
      }

    // Output Format: (n_nationkey, n_name, yearMonth, count)
    results.foreach { case (nkey, nname, ym, cnt) =>
      println(s"($nkey,$nname,$ym,$cnt)")
    }
  }

  /**
   * Text Version
   */
  def runText(sc: SparkContext, input: String): Array[(Int, String, String, Long)] = {
    val lPath = s"$input/lineitem.tbl"
    val oPath  = s"$input/orders.tbl"
    val cPath  = s"$input/customer.tbl"
    val nPath  = s"$input/nation.tbl"

    val nations = sc.textFile(nPath)
    val customers = sc.textFile(cPath)
    val orders = sc.textFile(oPath)
    val lines = sc.textFile(lPath)

    // nation: n_nationkey (1st column, index 0), n_name (2nd column, index 1) - Only need CANADA and USA
    val nationMap = nations
      .map(_.split("\\|", -1))
      .filter(_.length > 1)
      .map { fields =>
        val n_nationkey = fields(0).toInt
        val n_name = fields(1)
        (n_nationkey, n_name)
      }
      .collect()
      .toMap

    val desired = Set("CANADA", "UNITED STATES")
    val filteredNationMap = nationMap.filter { case (_, name) => desired.contains(name) }

    // customer: c_custkey (1st column, index 0), c_nationkey (4th column, index 3)
    val customerMap = customers
      .map(_.split("\\|", -1))
      .filter(_.length > 3)
      .map { fields =>
        val c_custkey = fields(0).toLong
        val c_nationkey = fields(3).toInt
        (c_custkey, c_nationkey)
      }
      .collect()
      .toMap

    val nationBroadcast = sc.broadcast(filteredNationMap)
    val customerBroadcast = sc.broadcast(customerMap)

    // orders: o_orderkey (1st column, index 0), o_custkey (2nd column, index 1)
    val ordersByOrder = orders
      .map(_.split("\\|", -1))
      .filter(_.length > 1)
      .map { fields =>
        val o_orderkey = fields(0).toLong
        val o_custkey = fields(1).toLong
        (o_orderkey, o_custkey)
      }

    // lineitem: l_orderkey (1st column, index 0), l_shipdate (11th column, index 10)
    val lineitemByOrder = lines
      .map(_.split("\\|", -1))
      .filter(_.length > 10)
      .map { fields =>
        val l_orderkey = fields(0).toLong
        val shipdate = fields(10) // YYYY-MM-DD
        (l_orderkey, shipdate)
      }

    // Join lineitem and orders with cogroup to relate eeach lineitem with its customer key
    val joined = lineitemByOrder.cogroup(ordersByOrder)
    // joined: RDD[(orderkey, (Iterable[shipdate], Iterable[custkey]))]

    // We want the Year-Month from the date
    val nationMonth = joined.flatMap { case (_, (shipdates, custkeys)) =>
      if (shipdates.nonEmpty && custkeys.nonEmpty) {
        val customerMapValue = customerBroadcast.value
        val nationMapValue = nationBroadcast.value

        for {
          shipdate <- shipdates.iterator
          custkey <- custkeys.iterator

          cNationKey <- customerMapValue.get(custkey)
          // lookup (n_nationkey, n_name) and filter CANADA / US
          nName <- nationMapValue.get(cNationKey)
        } yield {
          val yearMonth = shipdate.substring(0, 7) // YYYY-MM
          ((cNationKey, nName, yearMonth), 1L)
        }
      } else {
        Seq.empty[((Int, String, String), Long)]
      }
    }

    // Now, Compute the Counts and Sort by nationKey, then Date
    val counts = nationMonth
      .reduceByKey(_ + _)
      .map { case ((nkey, nname, ym), cnt) => (nkey, nname, ym, cnt) }
      .sortBy({ case (nkey, nname, ym, _) => (nkey, ym) }, ascending = true)

    counts.collect()
  }

  /**
   * Parquet Version: reading with SparkSession.read.parquet and using RDDs
   */
  def runParquet(sc: SparkContext, input: String): Array[(Int, String, String, Long)] = {
    val lPath = s"$input/lineitem"
    val oPath  = s"$input/orders"
    val cPath  = s"$input/customer"
    val nPath  = s"$input/nation"

    val sparkSession = SparkSession.builder().getOrCreate()
    // DataFrames
    val lineitemDF = sparkSession.read.parquet(lPath)
    val ordersDF = sparkSession.read.parquet(oPath)
    val customerDF = sparkSession.read.parquet(cPath)
    val nationDF = sparkSession.read.parquet(nPath)
    // RDDs (the used .select's are allowed in the assignment in this case)
    val lineitemRDD = lineitemDF.select("l_orderkey", "l_shipdate").rdd
    val ordersRDD = ordersDF.select("o_orderkey", "o_custkey").rdd
    val customerRDD = customerDF.select("c_custkey", "c_nationkey").rdd
    val nationRDD = nationDF.select("n_nationkey", "n_name").rdd

    val nationMap = nationRDD
      .map { row =>
        val n_nationkey = row.get(0) match {
          case i: Int => i
          case l: Long => l.toInt
          case s: String => s.toInt
          case other => other.toString.toInt
        }
        val n_name = row.getString(1)
        (n_nationkey, n_name)
      }
      .collect()
      .toMap

    val desired = Set("CANADA", "UNITED STATES")
    val filteredNationMap = nationMap.filter { case (_, name) => desired.contains(name) }

    val customerMap = customerRDD
      .map { row =>
        val c_custkey = row.get(0) match {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        val c_nationkey = row.get(1) match {
          case i: Int => i
          case l: Long => l.toInt
          case s: String => s.toInt
          case other => other.toString.toInt
        }
        (c_custkey, c_nationkey)
      }
      .collect()
      .toMap

    val nationBroadcast = sc.broadcast(filteredNationMap)
    val customerBroadcast = sc.broadcast(customerMap)

    val ordersByOrder = ordersRDD
      .map { row =>
        val o_orderkey = row.get(0) match {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        val o_custkey = row.get(1) match {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        (o_orderkey, o_custkey)
      }

    val lineitemByOrder = lineitemRDD
      .map { row =>  // We are not filtering here, so map instead of flatmap
        val orderkeyAny = row.get(0)
        val shipVal = row.get(1)

        val l_orderkey = orderkeyAny match {
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

        (l_orderkey, shipdate)

      }

    // "Lineitem and orders wont fit in memory" -> joint with reduce-side using cogroup
    val joined = lineitemByOrder.cogroup(ordersByOrder)

    val nationMonth = joined.flatMap { case (_, (shipdates, custkeys)) =>
      if (shipdates.nonEmpty && custkeys.nonEmpty) {
        val customerMapValue = customerBroadcast.value
        val nationMapValue = nationBroadcast.value

        for {
          shipdate <- shipdates.iterator
          custkey <- custkeys.iterator
          cNationKey <- customerMapValue.get(custkey)
          nName <- nationMapValue.get(cNationKey)
        } yield {
          val yearMonth = shipdate.substring(0, 7)
          ((cNationKey, nName, yearMonth), 1L)
        }
      } else {
        Seq.empty[((Int, String, String), Long)]
      }
    }

    val counts = nationMonth
      .reduceByKey(_ + _)
      .map { case ((nkey, nname, ym), cnt) => (nkey, nname, ym, cnt) }
      .sortBy({ case (nkey, _, ym, _) => (nkey, ym) }, ascending = true)

    counts.collect()
  }
}
