package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._


class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model path", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplySpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("Apply Spam Classifier")
    val sc = new SparkContext(conf)

    // Delete output directory if it exists
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)


    // STEP 1: Load Trained Model = Extract its information (features and weights tupples)
    val model = sc.textFile(args.model())
        .map(line => {
            // Parse tuples (feature, weight)
            val trimmed = line.trim.substring(1, line.trim.length - 1)  // Remove ( and )
            val parts = trimmed.split(",")
            val feature = parts(0).toInt
            val weight = parts(1).toDouble
            (feature, weight)
        }).collectAsMap()

    // STEP 2: Broadcast the model to the workers
    val wBroad = sc.broadcast(model)

    // STEP 3: Read test data and apply classifier model
    val predictions = sc.textFile(args.input())
        .map(line => {
            // Parse instances (same format as we did in Training)
            val parts = line.split(" ")
            val docid = parts(0)
            val label = parts(1) // Keep label for output (NOT used for prediction)
            val features = parts.slice(2, parts.length).map(_.toInt)
            
            // Compute spamminess score using the model
            val w = wBroad.value
            var score = 0d
            features.foreach(f => if (w.contains(f)) score += w(f))

            // Prediction based on score
            val prediction = if (score > 0) "spam" else "ham"

            // Output Format:
            (docid, label, score, prediction)
        })

        // STEP 4: Save predictions
        predictions.saveAsTextFile(args.output())
  }
}