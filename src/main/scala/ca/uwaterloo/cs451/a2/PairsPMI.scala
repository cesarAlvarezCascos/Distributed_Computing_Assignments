package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfPairsPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairsPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val threshold = args.threshold()
    val maxWords = 40

    // Delete output directory if exists
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())

    // Count total number of lines
    val N = textFile.count()

    // Job 1: Count individual words: number of lines containing that word
        // This is the 1st pass over the data
    val wordCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val limit = math.min(maxWords, tokens.length)
        val uniqueWords = tokens.take(limit).toSet
        uniqueWords.map(w => (w, 1))
      })
      // Reducer: sums every count of same words
      .reduceByKey(_ + _, args.reducers())
      .collectAsMap() // Collect as map for broadcasting

    // Broadcast word counts to workers
    val bcWordCounts = sc.broadcast(wordCounts)

    // Job 2: Count co-occurring pairs
        // This is the 2nd pass over the data
    val pairs = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val limit = math.min(maxWords, tokens.length)
        val uniqueWords = tokens.take(limit).distinct // We cannot use toSet, although it is faster and cheaper, but we need to keep the order for the later 'for' loops 

        // Generate all pairs (w1, w2) where w1 != w2
        for {
          i <- 0 until uniqueWords.length
          j <- 0 until uniqueWords.length
          if i != j
        } yield ((uniqueWords(i), uniqueWords(j)), 1)
      })
      // Reducer: sums every count of same co-occuring pairs
      .reduceByKey(_ + _, args.reducers())

    // Compute PMI for each pair
    val pmiResults = pairs
        // Threshold
      .filter { case (_, count) => count >= threshold }
      .map { case ((w1, w2), countPair) =>
        val countW1 = bcWordCounts.value.getOrElse(w1, 0)
        val countW2 = bcWordCounts.value.getOrElse(w2, 0)

        if (countW1 > 0 && countW2 > 0) {
          val numerator = countPair.toDouble * N.toDouble
          val denominator = countW1.toDouble * countW2.toDouble
          val pmi = math.log10(numerator / denominator)
          ((w1,w2),(pmi,countPair))
        } else {
          ((w1,w2),(0.0,0))
        }
      }
      // Remove those '((w1,w2),(0.0,0))' pairs
      .filter { case (_, (pmi, count)) => count > 0 }

    pmiResults.saveAsTextFile(args.output())

  }
}
