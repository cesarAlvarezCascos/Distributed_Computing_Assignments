package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfStripes(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfStripes(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    // Delete output directory if exists
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    // Generate Stripes: for each word, one Map of consecutive words and their counts
    val stripes = textFile.flatMap(line => {
      val tokens = tokenize(line)  // Tokenize line
      if (tokens.length > 1) {
        tokens.sliding(2).map { // Generate all 2-sized windows (bigrams) in the line
          case List(w1, w2) => (w1, Map(w2 -> 1.0f))
          case _ => ("", Map[String, Float]())
        }.toList
      } else List()
    })

    // Sum maps for each word
    val combined = stripes.reduceByKey((m1, m2) => { // Reducer (groups by equal bigrams)
      (m1.keySet ++ m2.keySet).map { k =>  // Sum the set of keys of both words' maps
        k -> (m1.getOrElse(k, 0.0f) + m2.getOrElse(k, 0.0f))
      }.toMap
    }, args.reducers()) // Specify number of reducers

    // Compute Relative Frequencies
    val relativeFreqs = combined.map { case (w1, stripe) =>
      val marginal = stripe.values.sum
      val relative = stripe.map { case (w2, count) => (w2, count / marginal) }
      (w1, relative)
    }

    relativeFreqs.saveAsTextFile(args.output())
  }
}
