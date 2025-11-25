package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

import org.apache.spark.sql.SparkSession

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "ship date YYYY-MM-DD", required = true)

  val text = opt[Boolean](descr = "use text input", required = false, default = Some(false))
  val parquet = opt[Boolean](descr = "use parquet input", required = false, default = Some(false))
  verify()
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Q6Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    val inputPath = args.input()
    val date = args.date()

    val useText = args.text()
    val useParquet = args.parquet()

    if (useText == useParquet) {
      // Only one must be true
      log.warn("Specify exactly one of --text or --parquet")
    }

  
    val results: Array[(String, String, Double, Double, Double, Double, Double, Double, Double, Long)] =
      if (useText) {
        runText(sc, inputPath, date)
      } else {
        runParquet(sc, inputPath, date)
      }

    // Output Format: (l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
    results.foreach {
      case (rf, ls, sumQty, sumBase, sumDiscPrice, sumCharge, avgQty, avgPrice, avgDisc, cnt) =>
        println(s"($rf,$ls,$sumQty,$sumBase,$sumDiscPrice,$sumCharge,$avgQty,$avgPrice,$avgDisc,$cnt)")
    }
  }

  // Aggregation by key: 
  // (sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_qty_for_avg, sum_price_for_avg, sum_disc_for_avg, count)
  // I also use sums for the values for which I will compute average
  type Agg = (Double, Double, Double, Double, Double, Double, Double, Long)

  /**
   * Text Version
   */
  def runText(sc: SparkContext, input: String, date: String) = {

    val lPath = s"$input/lineitem.tbl" // we only use one table for this query

    val lines = sc.textFile(lPath)

    val lineitem = lines
      .map(_.split("\\|", -1))
      .filter(fields => fields.length > 10 && fields(10) == date) // filter by specified date

    val usedValues = lineitem.map { fields =>
      val l_returnflag = fields(8)
      val l_linestatus = fields(9)
      val l_quantity = fields(4).toDouble
      val l_extendedprice = fields(5).toDouble
      val l_discount  = fields(6).toDouble
      val l_tax = fields(7).toDouble

      val disc_price = l_extendedprice * (1.0 - l_discount)
      val charge = disc_price * (1.0 + l_tax)

      val key = (l_returnflag, l_linestatus)
      val agg: Agg =
        (l_quantity, // sum_qty
         l_extendedprice, // sum_base_price
         disc_price, // sum_disc_price
         charge, // sum_charge
         l_quantity, // sum_qty_for_avg
         l_extendedprice, // sum_price_for_avg
         l_discount, // sum_disc_for_avg
         1L) // count

      (key, agg)
    }

    // Initial accumulator for aggregateByKey
    val zero: Agg = (0.0,0.0,0.0,0.0, 0.0,0.0,0.0, 0L)

    // Now we compute (sum)
    val aggregated = usedValues.aggregateByKey(zero)(
      // Update the accumulator with new row
      (acc: Agg, v: Agg) => {
        val (sq, sb, sdp, sch, aq, ap, ad, c) = acc
        val (vq, vb, vdp, vch, vq2, vp2, vd2, c2) = v
        (sq + vq, sb + vb, sdp + vdp, sch + vch, aq + vq2, ap + vp2, ad + vd2, c + c2)
      },
      //  Combine accumulators
      (a: Agg, b: Agg) => {
        val (sq, sb, sdp, sch, aq, ap, ad, c) = a
        val (sq2,sb2,sdp2,sch2,aq2,ap2,ad2,c2) = b
        (sq + sq2, sb + sb2, sdp + sdp2, sch + sch2, aq + aq2, ap + ap2, ad + ad2, c + c2)
      }
    )

    val result = aggregated.map { case ((rf, ls), (sumQty, sumBase, sumDiscP, sumCh, sumQtyAvg, sumPriceAvg, sumDiscAvg, cnt)) =>
      val avgQty   = sumQtyAvg / cnt
      val avgPrice = sumPriceAvg / cnt
      val avgDisc  = sumDiscAvg / cnt
      (rf, ls, sumQty, sumBase, sumDiscP, sumCh, avgQty, avgPrice, avgDisc, cnt)
    }

    result.collect()
  }

  /**
   * Parquet Version: reading with SparkSession.read.parquet and using RDDs
   */
  def runParquet(sc: SparkContext, input: String, date: String) = {

    val lPath = s"$input/lineitem"

    val sparkSession = SparkSession.builder().getOrCreate()
    // DataFrame
    val lineitemDF = sparkSession.read.parquet(lPath)
    // RDD with the allowed .select for filtering columns to be used
    val lineitemRDD = lineitemDF.select("l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_shipdate").rdd

    val usedValues = lineitemRDD.flatMap { row =>
      val shipVal = row.get(6)
      val shipdate = shipVal match {
        case d: java.sql.Date => d.toString
        case s: String => s
        case other => other.toString
      }
      if (shipdate != date) {
        None
      } else {
        val l_returnflag = row.getString(0)
        val l_linestatus = row.getString(1)

        val l_quantity = row.get(2) match {
          case d: Double => d
          case f: Float => f.toDouble
          case l: Long => l.toDouble
          case i: Int => i.toDouble
          case s: String => s.toDouble
          case other => other.toString.toDouble
        }

        val l_extendedprice = row.get(3) match {
          case d: Double => d
          case f: Float => f.toDouble
          case l: Long => l.toDouble
          case i: Int => i.toDouble
          case s: String => s.toDouble
          case other => other.toString.toDouble
        }

        val l_discount = row.get(4) match {
          case d: Double => d
          case f: Float => f.toDouble
          case s: String => s.toDouble
          case other => other.toString.toDouble
        }

        val l_tax = row.get(5) match {
          case d: Double => d
          case f: Float => f.toDouble
          case s: String => s.toDouble
          case other => other.toString.toDouble
        }

        val disc_price = l_extendedprice * (1.0 - l_discount)
        val charge = disc_price * (1.0 + l_tax)

        val key = (l_returnflag, l_linestatus)
        val agg: Agg =
          (l_quantity,
           l_extendedprice,
           disc_price,
           charge,
           l_quantity,
           l_extendedprice,
           l_discount,
           1L)

        Some((key, agg))
      }
    }

    val zero: Agg = (0.0,0.0,0.0,0.0, 0.0,0.0,0.0, 0L)

    val aggregated = usedValues.aggregateByKey(zero)(
      (acc: Agg, v: Agg) => {
        val (sq, sb, sdp, sch, aq, ap, ad, c) = acc
        val (vq, vb, vdp, vch, vq2, vp2, vd2, c2) = v
        (sq + vq, sb + vb, sdp + vdp, sch + vch, aq + vq2, ap + vp2, ad + vd2, c + c2)
      },
      (a: Agg, b: Agg) => {
        val (sq, sb, sdp, sch, aq, ap, ad, c) = a
        val (sq2,sb2,sdp2,sch2,aq2,ap2,ad2,c2) = b
        (sq + sq2, sb + sb2, sdp + sdp2, sch + sch2, aq + aq2, ap + ap2, ad + ad2, c + c2)
      }
    )

    val result = aggregated.map { case ((rf, ls), (sumQty, sumBase, sumDiscP, sumCh, sumQtyAvg, sumPriceAvg, sumDiscAvg, cnt)) =>
      val avgQty   = sumQtyAvg / cnt
      val avgPrice = sumPriceAvg / cnt
      val avgDisc  = sumDiscAvg / cnt
      (rf, ls, sumQty, sumBase, sumDiscP, sumCh, avgQty, avgPrice, avgDisc, cnt)
    }
      .sortBy({ case (rf, ls, _, _, _, _, _, _, _, _) => (rf, ls) }, ascending = true)

    result.collect()
  }
}