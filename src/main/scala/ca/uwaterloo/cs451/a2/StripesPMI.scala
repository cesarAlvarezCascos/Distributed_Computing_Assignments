package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfStripesPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(1))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfStripesPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)

    val threshold = args.threshold()
    val maxWords = 40

    // Delete output directory if exists
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    // Count total number of lines
    val N = textFile.count()

    // Job 1: Count individual words (number of lines each word appears in)
    val wordCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val limit = math.min(maxWords, tokens.length)
        val uniqueWords = tokens.take(limit).toSet
        uniqueWords.map(w => (w, 1))
      })
      .reduceByKey(_ + _, args.reducers())
      .collectAsMap()

    // Broadcast word counts to workers
    val bcWordCounts = sc.broadcast(wordCounts)

    // Job 2: Generate stripes (for each word, a map of co-occurring words) in each line
    val stripes = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val limit = math.min(maxWords, tokens.length)
        val uniqueWords = tokens.take(limit).distinct

        // For each word 'w', create a stripe of its co-occurring words
        uniqueWords.map { w =>
            // Filter looking for every distinct word, assigning 1 to each of them
          val coOccurring = uniqueWords.filter(_ != w).map(w2 => (w2, 1)).toMap         
          // Stripe for word 'w'
          (w, coOccurring)
        }
      })

      // Combine stripes by merging maps 
      .reduceByKey((m1, m2) => { // m1 and m2 are stripes of the same key (word)
        (m1.keySet ++ m2.keySet).map { k =>
          k -> (m1.getOrElse(k, 0) + m2.getOrElse(k, 0))
          // Combines the keys of both stripes m1 and m2, then it goes along every key in this new map and sum their values
        }.toMap
      }, args.reducers())


    // Compute PMI for each word's stripe
    val pmiResults = stripes
      .map { case (w1, stripe) =>
        val countW1 = bcWordCounts.value.getOrElse(w1, 0)

        // For each co-occurring word within the stripe
        val pmiStripe = stripe
          .filter { case (_, count) => count >= threshold }
          .map { case (w2, countPair) =>
            val countW2 = bcWordCounts.value.getOrElse(w2, 0)

            if (countW1 > 0 && countW2 > 0) {
              val numerator = countPair.toDouble * N.toDouble
              val denominator = countW1.toDouble * countW2.toDouble
              val pmi = math.log10(numerator / denominator)
              (w2, (pmi, countPair))
            } else {
              (w2, (0.0, 0))
            }
          }
          .filter { case (_, (_, count)) => count > 0}
          .toMap

        (w1, pmiStripe)
      }
      .filter {case (_, stripe) => stripe.nonEmpty }

    pmiResults.saveAsTextFile(args.output())
  }
}
