/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package ca.uwaterloo.cs451.a4

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.rogach.scallop._


object PageRank_Original {
  val log = Logger.getLogger(getClass().getName())
  val DAMPING = 0.85f
  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, iterations)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val iterations = opt[Int](descr = "number of iterations", required = true, validate = (_ > 0))
        // Iterations refers to number of times we repeat the PageRank computation, each iter. updates ranks, making them converg to more stable values when many iters.
    val partitions = opt[Int](descr = "number of partitions (0 = determine from input)", required = false, default = Some(0))
    verify()
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of partitions: " + args.partitions())
    log.info("Numer of iterations: " + args.iterations())
    val conf = new SparkConf().setAppName("PageRank_Original")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    val iterations = args.iterations()

    // Delete output directory if existing
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    // if number of partitions argument is not specified, use the same number as the input file
        // it means the number of partitions of the created RDD when reading the textFile (data blocks processed by Spark)
    val partitions = if (args.partitions() > 0) args.partitions() else textFile.getNumPartitions

    // Build Adjacency List
    val adjList = textFile.map(line => {
       val parts = line.split("\\s+")
       (parts(0).toInt, parts(1).toInt)}).distinct.groupByKey(partitions).cache 
       // toInt means the input file contains numbers, we take parts 1 and 2 assuming the file contains pairs in each line
       // As they are integers and we eliminate duplicates, it makes sense to think about a graph and nodes, where each line in the textFile is one connection
       // .groupByKey groups by the key (origin node), and partitions specify number of partitions to process in parallell (doesn't influences the logic grouping)
       // cache saves the RDD in memory, as it will be reused in each iteration
    
    val N = adjList.count // Amount of different nodes as origin (nodes with destinations, # of unique keys are the size of the graph)

    // Initial Normalized Ranks
    var ranks = adjList.mapValues(x => 1.0f / N) // Being x the value of the Map for each key, what is to say, the destination nodes. 
                                                            // It is replaced by normalized rank, same initial rank for eeevery origin node

    // 'ranks' is the Adjacency List with the initial rank as value for each origin node
    // 'adjList' is the Adjacency List with the destinations as value for each origin node

    // Compute Ranks
    for (i <- 1 to iterations) {
      val contribs = adjList.join(ranks).values.flatMap{case (adj, rank) =>  // '.values' We take out the key (the origin) of the resultant join list, keeping destinations and rank
          val size = adj.size // Amount of destinations per node ('adj' is the list of destinations) 
          adj.map(_ -> rank / size) // For each element in 'adj' distribute the rank equitatively, creating a pair of (dest. node, its rank)
      }
      // contribs is now a list of all nodes that were a destination, with their assigned ranks (flatMap joins into a list of pairs all the adj.maps computed)

      // reduceByKey: combine 'contribs' by key, suming the different ranks assigned to each nodes by different origins
      // mapValues applies the formula, being _ the rank of each node
      ranks = contribs.reduceByKey(_ + _, partitions).mapValues( _ * DAMPING + (1 - DAMPING) / N)
    }
    ranks.saveAsTextFile(args.output())
  }
}