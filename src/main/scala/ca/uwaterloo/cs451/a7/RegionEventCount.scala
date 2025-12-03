package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class RegionEventCountConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object RegionEventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new RegionEventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("RegionEventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    // BUILD BOUNDING BOXES 
    val goldmanMinLon = -74.0144185
    val goldmanMaxLon = -74.013777
    val goldmanMinLat = 40.7138745
    val goldmanMaxLat = 40.7152275
    val citigMinLon = -74.012083
    val citigMaxLon = -74.009867
    val citigMinLat = 40.720053
    val citigMaxLat = 40.7217236

    // FILTER DROPOFFS WITHIN THE BOUNDING BOXES & MAP
    val regionStream = stream.flatMap { line => val parts = line.split(",")

        if (parts.length < 12){
            None
        }else{
            val recordType = parts(0)

            // Get longitude and lattitude (different for Green/yellow trips)
            val (lonStr, latStr) = 
                if (recordType == "yellow"){
                    (parts(10), parts(11))
                }else if (recordType == "green"){
                    (parts(8), parts(9))
                }else{
                    ("", "")
                }
            
            if (lonStr.isEmpty() || latStr.isEmpty()){
                None
            }else{
                try{
                    val lon = lonStr.toDouble
                    val lat = latStr.toDouble

                    val inGoldman = 
                        lon > goldmanMinLon && lon < goldmanMaxLon &&
                        lat > goldmanMinLat && lat < goldmanMaxLat

                    val inCitigroup = 
                        lon > citigMinLon && lon < citigMaxLon &&
                        lat > citigMinLat && lat < citigMaxLat

                    if (inGoldman){
                        Some(("goldman", 1))
                    }else if (inCitigroup){
                        Some(("citigroup", 1))
                    }else{
                        None
                    }
                } catch{
                    case _: NumberFormatException => None
                }
            }
        }
    }

    val wc = regionStream
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
