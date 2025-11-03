package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._


class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model output path", required = true)
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new TrainSpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("Train Spam Classifier")
    val sc = new SparkContext(conf)

    // Delete output directory if it exists
    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // STEP 1: Read data forcing 1 unique partition (deterministic order), avoiding Spark distributing data randomly
      // As SGD depends on the order, we need sequential processing
    val textFile = sc.textFile(args.input(), 1)

    // STEP 2: Mappers are just parsing the feature vectors and pushing them over the reducer side. Emit '0' as dummy key
    val trained = textFile.map(line => {   // mapper
      // Parse input
      val parts = line.split(" ")
      val docid = parts(0)

      val isSpam = if (parts(1) == "spam") 1 else 0  // Equation (12)
      val features = parts.slice(2, parts.length).map(_.toInt)
      
      // Dummy key 0 - all records with key 0 -> all goes to the unique reducer thanks to the groupByKey
      (0, (docid, isSpam, features))
    }).groupByKey(1)  // Group in 1 reducer
    // Then run the trainer...

    // STEP 3: Train with SGD

    .flatMap { case (key, instances) =>  // reducer

      // w is the weight vector (make sure it is within scope) -- Mutable object for updates
        // weights are for the features, measuring how much it contributes to making it spam
      val w = scala.collection.mutable.Map[Int, Double]()
      // val w = Map[Int, Double]()
      
      // Scores a document based on its list of features
      def spamminess(features: Array[Int]): Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
      }
      
      // This is the main learner:
        // Constant delta (learning rate)
      val delta = 0.002
      
      // For each instance (SGD loop)
      instances.foreach { case (docid, isSpam, features) =>


        // Update the weights as follows
        val score = spamminess(features)
        val prob = 1.0 / (1 + math.exp(-score))  // Prob. in Eq (11)
        
        features.foreach(f => {
          if (w.contains(f)) {
            w(f) += (isSpam - prob) * delta
          } else {
            w(f) = (isSpam - prob) * delta
          }
        })
      }
      
      // Return model as tuples (feature, weight)
      w.toSeq
    }

    // STEP 4: Save model
    trained.saveAsTextFile(args.model())
  }
}