package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date YYYY-MM-DD", required = true)

  val text = opt[Boolean](descr = "use text input", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "use parquet input", required = false, default = Some(false))
  verify()
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Q4Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

    val inputPath = args.input()
    val date = args.date()

    val useText = args.text()
    val useParquet = args.parquet()

    if (useText == useParquet) {
      // Only one must be true
      log.warn("Specify exactly one of --text or --parquet")
    }

    val results: Array[(Int, String, Long)] =
      if (useText) {
        runText(sc, inputPath, date)
      } else {
        runParquet(sc, inputPath, date)
      }

    // Output Format: (n_nationkey,n_name,count)
    results.foreach { case (nkey, nname, cnt) =>
      println(s"($nkey,$nname,$cnt)")
    }
  }

  /**
   * Text Version: reduce-side join lineitem - orders with cogroup and hash join (broadcast) for customer and nation
   */
  def runText(sc: SparkContext, input: String, date: String): Array[(Int, String, Long)] = {
    val lPath = s"$input/lineitem.tbl"
    val oPath = s"$input/orders.tbl"
    val cPath = s"$input/customer.tbl"
    val nPath = s"$input/nation.tbl"

    val nations = sc.textFile(nPath)
    val customers = sc.textFile(cPath)
    val orders = sc.textFile(oPath)
    val lines = sc.textFile(lPath)

    // nation: n_nationkey (1st column, index 0), n_name (2nd column, index 1
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

    val nationBroadcast = sc.broadcast(nationMap)
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
    val lineitemByOrder  = lines
      .map(_.split("\\|", -1))
      .filter(fields => fields.length > 10 && fields(10) == date)
      .map { fields =>
        val l_orderkey = fields(0).toLong
        (l_orderkey, 1)      // Flag for that date
      }
      .distinct()          

    // "Lineitem and orders wont fit in memory" -> joint with reduce-side using cogroup
    val joined = lineitemByOrder.cogroup(ordersByOrder)
    // joined: RDD[(Long, (Iterable[Int], Iterable[Long]))]

    // For each orderkey in lineitem and orders, we get its o_custkey
    val custByOrder = joined.flatMap { case (orderkey, (liIter, oIter)) =>
      if (liIter.nonEmpty && oIter.nonEmpty) {
        oIter.map(custkey => (orderkey, custkey))
      } else {
        Seq.empty[(Long, Long)]
      }
    }

    // Map (order, cust) pairs to (nationKey, nationName) using broadcast maps
    val nationPairs = custByOrder.flatMap { case (_, custkey) =>
      val customerMapValue = customerBroadcast.value
      val nationMapValue   = nationBroadcast.value

      customerMapValue.get(custkey) match {
        case Some(nationKey) => nationMapValue.get(nationKey) match {
            case Some(nationName) => Some((nationKey, nationName))
            case None => None
          }
        case None => None
      }
    }

    // Count items per nation: count(*) & Group By n_nationkey, n_name
    val countsByNation = nationPairs
      .map(n => (n, 1L))
      .reduceByKey(_ + _)
      .map { case ((nkey, nname), cnt) => (nkey, nname, cnt) }

    // sort by key ascending
    countsByNation
      .sortBy({ case (nkey, _, _) => nkey }, ascending = true)
      .collect()
  }

  /**
   * Parquet Version: reading with SparkSession.read.parquet and using RDDs
   */
  def runParquet(sc: SparkContext, input: String, date: String): Array[(Int, String, Long)] = {
    val lPath = s"$input/lineitem"
    val oPath  = s"$input/orders"
    val cPath  = s"$input/customer"
    val nPath  = s"$input/nation"

    val spark = SparkSession.builder().getOrCreate()
    // DataFrames
    val lineitemDF = spark.read.parquet(lPath)
    val ordersDF   = spark.read.parquet(oPath)
    val customerDF = spark.read.parquet(cPath)
    val nationDF   = spark.read.parquet(nPath)
    // RDDs (the used .select's are allowed in the assignment in this case)
    val lineitemRDD = lineitemDF.select("l_orderkey", "l_shipdate").rdd
    val ordersRDD   = ordersDF.select("o_orderkey", "o_custkey").rdd
    val customerRDD = customerDF.select("c_custkey", "c_nationkey").rdd
    val nationRDD   = nationDF.select("n_nationkey", "n_name").rdd

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

    val nationBroadcast = sc.broadcast(nationMap)
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
      .flatMap { row =>
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

        if (shipdate == date) Some((l_orderkey, 1)) else None
      }
      .distinct()

    // "Lineitem and orders wont fit in memory" -> joint with reduce-side using cogroup
    val joined = lineitemByOrder.cogroup(ordersByOrder)

    val custByOrder = joined.flatMap { case (orderkey, (liIter, oIter)) =>
      if (liIter.nonEmpty && oIter.nonEmpty) {
        oIter.map(custkey => (orderkey, custkey))
      } else {
        Seq.empty[(Long, Long)]
      }
    }

    val nationPairs = custByOrder.flatMap { case (_, custkey) =>
      val customerMapLocal = customerBroadcast.value
      val nationMapLocal   = nationBroadcast.value

      customerMapLocal.get(custkey) match {
        case Some(nationKey) =>
          nationMapLocal.get(nationKey) match {
            case Some(nationName) =>
              Some((nationKey, nationName))
            case None =>
              None
          }
        case None =>
          None
      }
    }

    val countsByNation = nationPairs
      .map(n => (n, 1L))
      .reduceByKey(_ + _)
      .map { case ((nkey, nname), cnt) => (nkey, nname, cnt) }

    countsByNation
      .sortBy({ case (nkey, _, _) => nkey }, ascending = true)
      .collect()
  }
}
