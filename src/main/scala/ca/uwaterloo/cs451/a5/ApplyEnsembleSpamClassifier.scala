package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._


class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "model base path", required = true)
  val method = opt[String](descr = "ensemble method: average or vote", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplyEnsembleSpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("Apply Ensemble Spam Classifier")
    val sc = new SparkContext(conf)

    // Delete output directory if it exists
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)


    // STEP 1: Load the 3 Models

    // helper function to parse model file
    def loadModel(modelPath: String): Map[Int, Double] = {
        sc.textFile(modelPath)
        .map(line => {
            // Parse tuples (feature, weight)
            val trimmed = line.trim.substring(1, line.trim.length - 1)  // Remove ( and )
            val parts = trimmed.split(",")
            val feature = parts(0).toInt
            val weight = parts(1).toDouble
            (feature, weight)
        }).collectAsMap()
        .toMap
    }
    
    // Hard-Coded part files
    val model1 = loadModel(args.model() + "/part-00000") // group_x
    val model2 = loadModel(args.model() + "/part-00001") // group_y
    val model3 = loadModel(args.model() + "/part-00002") // britney

    // STEP 2: Broadcast all models to the workers
    val model1Broad = sc.broadcast(model1)
    val model2Broad = sc.broadcast(model2)
    val model3Broad = sc.broadcast(model3)

    // STEP 3: helper function to compute spamminess of a model
    def spamminess(features: Array[Int], model: Map[Int, Double]): Double = {
        var score = 0d
        features.foreach(f => if (model.contains(f)) score += model(f))
        score
    }


    // STEP 4: Read test data and apply ensemble method
    val method = args.method()

    val predictions = sc.textFile(args.input())
        .map(line => {
            // Parse instances (same format as we did in Training)
            val parts = line.split(" ")
            val docid = parts(0)
            val label = parts(1) // Keep label for output (NOT used for prediction)
            val features = parts.slice(2, parts.length).map(_.toInt)
            
            // Get models' weights from broadcast
            val w1 = model1Broad.value
            val w2 = model2Broad.value
            val w3 = model3Broad.value

            // Compute spamminess score for each model
            val score1 = spamminess(features, w1)
            val score2 = spamminess(features, w2)
            val score3 = spamminess(features, w3)

            // Apply Ensemble Method to compute Predictions
            val (finalScore, prediction) = method match{
                case "average" =>
                    val avgScore = (score1 + score2 + score3) / 3.0
                    val pred = if (avgScore > 0) "spam" else "ham"
                    (avgScore, pred)

                case "vote" =>
                    val vote1 = if (score1 > 0) "spam" else "ham"
                    val vote2 = if (score2 > 0) "spam" else "ham"
                    val vote3 = if (score3 > 0) "spam" else "ham"
                    
                    val spamCount = List(vote1, vote2, vote3).count(_ == "spam")
                    val hamCount = 3 - spamCount
                    val pred = if (spamCount > hamCount) "spam" else "ham"

                    val voteScore = spamCount - hamCount
                    (voteScore.toDouble, pred)

                case _ =>
                    throw new IllegalArgumentException("Method must be 'average' or 'vote'")
            }

            // Output Format:
            (docid, label, finalScore, prediction)
        })

        // STEP 4: Save predictions
        predictions.saveAsTextFile(args.output())
  }
}