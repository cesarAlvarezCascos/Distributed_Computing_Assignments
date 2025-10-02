error id: file://<WORKSPACE>/src/main/scala/ca/uwaterloo/cs451/a2/BigramCountPairs.scala:`<none>`.
file://<WORKSPACE>/src/main/scala/ca/uwaterloo/cs451/a2/BigramCountPairs.scala
empty definition using pc, found symbol in pc: `<none>`.
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 284
uri: file://<WORKSPACE>/src/main/scala/ca/uwaterloo/cs451/a2/BigramCountPairs.scala
text:
```scala
package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfPairs(args: Seq[String]) extends ScallopConf(arg@@s) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairs(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)

    // Delete output directory if exists
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    
    // Generate pairs (bigram, count) and ((left_word, *), count) for marginals
    val pairs = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) {
          val bigramPairs = tokens.sliding(2).map(p => ((p(0), p(1)), 1.0f)).toList
          val marginalPairs = tokens.sliding(2).map(p => ((p(0), "*"), 1.0f)).toList
          // Concatenate lists
          bigramPairs ++ marginalPairs
        } else List()
      })
    
    // Sum occurences by Key
    val counts = pairs.reduceByKey(_ + _)
    
    // Compute Relative Frecuencies
    val relativeFreqs = counts
      .groupBy(_._1._1) // Group by the 1st element (word) of the 1st element (pair) 
      .flatMap { case (leftWord, group) =>
        val groupList = group.toList
        val marginal = groupList.find(_._1._2 == "*").map(_._2).getOrElse(0.0f)
        
        groupList.map { case ((w1, w2), count) =>
          if (w2 == "*") {
            ((w1, w2), count)
          } else {
            ((w1, w2), count / marginal)
          }
        }
      }
    
    relativeFreqs.saveAsTextFile(args.output())
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: `<none>`.