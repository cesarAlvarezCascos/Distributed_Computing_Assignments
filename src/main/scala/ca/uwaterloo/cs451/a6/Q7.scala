package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "limit date YYYY-MM-DD", required = true)

  val text = opt[Boolean](descr = "use text input", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "use parquet input", required = false, default = Some(false))
  verify()
}

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Q7Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    val inputPath = args.input()
    val date = args.date()

    val useText = args.text()
    val useParquet = args.parquet()

    if (useText == useParquet) {
      // Only one must be true
      log.warn("Specify exactly one of --text or --parquet")
    }

    val results: Array[(String, Long, Double, String, Int)] =
      if (useText) {
        runText(sc, inputPath, date)
      } else {
        runParquet(sc, inputPath, date)
      }

    // Output Format: (c_name, l_orderkey, revenue, o_orderdate, o_shippriority)
    results.foreach { case (cname, orderkey, revenue, odate, shipprio) =>
      println(s"($cname,$orderkey,$revenue,$odate,$shipprio)")
    }
  }

  /**
   * Text Version
   */
  def runText(sc: SparkContext, input: String, date: String) = {

    val lPath = s"$input/lineitem.tbl"
    val oPath  = s"$input/orders.tbl"
    val cPath  = s"$input/customer.tbl"

    val lines = sc.textFile(lPath)
    val customers = sc.textFile(cPath)
    val orders = sc.textFile(oPath)

    // customer: c_custkey (1) & c_name (2)
    val customerMap = customers
      .map(_.split("\\|", -1))
      .filter(_.length > 1)
      .map { fields =>
        val c_custkey = fields(0).toLong
        val c_name = fields(1)
        (c_custkey, c_name)
      }
      .collect()
      .toMap

    // customers fit in memory
    val customerBroadcast = sc.broadcast(customerMap)

    // orders: o_orderkey, o_custkey, o_orderdate & o_shippriority
    val ordersByKey = orders
      .map(_.split("\\|", -1))
      .filter(_.length > 7)
      .filter(fields => fields(4) < date) // o_orderdate < date
      .map { fields =>
        val o_orderkey = fields(0).toLong
        val o_custkey = fields(1).toLong
        val o_orderdate = fields(4)
        val o_shippriority= fields(7).toInt

        (o_orderkey, (o_custkey, o_orderdate, o_shippriority))
      }

    // lineitem: l_orderkey, l_extendedprice, l_discount & l_shipdate
    val lineitemByKey = lines
      .map(_.split("\\|", -1))
      .filter(_.length > 10)
      .filter(f => f(10) > date) // l_shipdate > date
      .map { fields =>
        val l_orderkey = fields(0).toLong
        val l_extendedprice = fields(5).toDouble
        val l_discount = fields(6).toDouble
        val revenue = l_extendedprice * (1.0 - l_discount)
        (l_orderkey, revenue)
      }

    // WHERE l_orderkey = o_orderkey
        // Reduce-side join lineitem with orders with cogroup
    val joined = ordersByKey.cogroup(lineitemByKey)
    // joined: RDD[(orderkey, (Iterable[(custkey,orderdate,shippriority)], Iterable[revenue]))]

    // GROUP BY c_name, l_orderkey, o_orderdate, o_shippriority
    val orderRevenue = joined.flatMap { case (orderkey, (ordersIter, revIter)) =>
      if (ordersIter.nonEmpty && revIter.nonEmpty) {
        val orderInfo = ordersIter.head
        val (custkey, odate, shippriority) = orderInfo
        val revenueSum = revIter.sum
        Some((orderkey, custkey, odate, shippriority, revenueSum))
      } else {
        None
      }
    }

    // WHERE c_custkey = o_custkey
        // Add c_name with hash join in memory using broadcast variable
    val withCustomer = orderRevenue.flatMap { case (orderkey, custkey, odate, shippriority, revenue) =>
      val custMap = customerBroadcast.value
      custMap.get(custkey).map { cname => (cname, orderkey, revenue, odate, shippriority)
      }
    }

    // Top 5 by descending revenue
    val top5 = withCustomer
      .sortBy({ case (_, _, revenue, _, _) => revenue }, ascending = false)
      .take(5)

    top5
  }

  /**
   * Parquet Version: reading with SparkSession.read.parquet and using RDDs
   */
  def runParquet(sc: SparkContext, input: String, date: String) = {

    val lPath = s"$input/lineitem"
    val oPath = s"$input/orders"
    val cPath = s"$input/customer"

    val sparkSession = SparkSession.builder().getOrCreate()
    // DataFrames
    val lineitemDF = sparkSession.read.parquet(lPath)
    val ordersDF = sparkSession.read.parquet(oPath)
    val customerDF = sparkSession.read.parquet(cPath)
    // RDDs with allowed .select for filtering
    val lineitemRDD = lineitemDF.select("l_orderkey", "l_extendedprice", "l_discount", "l_shipdate").rdd
    val ordersRDD = ordersDF.select("o_orderkey", "o_custkey", "o_orderdate", "o_shippriority").rdd
    val customerRDD = customerDF.select("c_custkey", "c_name").rdd

    val customerMap = customerRDD
      .map { row =>
        val c_custkey = row.get(0) match {
          case l: Long => l
          case i: Int => i.toLong
          case s: String => s.toLong
          case other => other.toString.toLong
        }
        val c_name = row.getString(1)
        (c_custkey, c_name)
      }
      .collect()
      .toMap

    val customerBroadcast = sc.broadcast(customerMap)

    val ordersByKey = ordersRDD
      .flatMap { row =>
        val orderkeyAny = row.get(0)
        val custkeyAny = row.get(1)
        val odateVal = row.get(2)
        val shipprioAny = row.get(3)

        val o_orderdate = odateVal match {
          case d: java.sql.Date => d.toString
          case s: String => s
          case other => other.toString
        }

        if (o_orderdate >= date) {  // Condition
          None
        } else {
          val o_orderkey = orderkeyAny match {
            case l: Long => l
            case i: Int => i.toLong
            case s: String => s.toLong
            case other => other.toString.toLong
          }
          val o_custkey = custkeyAny match {
            case l: Long => l
            case i: Int => i.toLong
            case s: String => s.toLong
            case other => other.toString.toLong
          }
          val o_shippriority = shipprioAny match {
            case i: Int => i
            case l: Long => l.toInt
            case s: String => s.toInt
            case other => other.toString.toInt
          }
          Some((o_orderkey, (o_custkey, o_orderdate, o_shippriority)))
        }
      }

    val lineitemByKey = lineitemRDD
      .flatMap { row =>
        val orderkeyAny = row.get(0)
        val extVal = row.get(1)
        val discVal = row.get(2)
        val shipVal = row.get(3)

        val l_shipdate = shipVal match {
          case d: java.sql.Date => d.toString
          case s: String => s
          case other => other.toString
        }

        if (l_shipdate <= date) { // Condition
          None
        } else {
          val l_orderkey = orderkeyAny match {
            case l: Long => l
            case i: Int => i.toLong
            case s: String => s.toLong
            case other => other.toString.toLong
          }
          val l_extendedprice = extVal match {
            case d: Double => d
            case f: Float => f.toDouble
            case l: Long => l.toDouble
            case i: Int => i.toDouble
            case s: String => s.toDouble
            case other => other.toString.toDouble
          }
          val l_discount = discVal match {
            case d: Double => d
            case f: Float => f.toDouble
            case s: String => s.toDouble
            case other => other.toString.toDouble
          }
          val revenue = l_extendedprice * (1.0 - l_discount)
          Some((l_orderkey, revenue))
        }
      }

    val joined = ordersByKey.cogroup(lineitemByKey)

    // GROUP BY c_name, l_orderkey, o_orderdate, o_shippriority
    val orderRevenue = joined.flatMap { case (orderkey, (ordersIter, revIter)) =>
      if (ordersIter.nonEmpty && revIter.nonEmpty) {
        val (custkey, odate, shipprio) = ordersIter.head
        val revenueSum = revIter.sum
        Some((orderkey, custkey, odate, shipprio, revenueSum))
      } else {
        None
      }
    }

    val withCustomer = orderRevenue.flatMap { case (orderkey, custkey, odate, shipprio, revenue) =>
      val cMap = customerBroadcast.value
      cMap.get(custkey).map { cname => (cname, orderkey, revenue, odate, shipprio)
      }
    }

    val top5 = withCustomer
      .sortBy({ case (_, _, revenue, _, _) => revenue }, ascending = false)
      .take(5)

    top5
  }
}
