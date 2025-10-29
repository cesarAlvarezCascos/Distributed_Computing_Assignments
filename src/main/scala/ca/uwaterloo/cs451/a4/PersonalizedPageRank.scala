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


object PersonalizedPageRank {
  val log = Logger.getLogger(getClass().getName())
  val DAMPING = 0.85f
  class Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, iterations)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val iterations = opt[Int](descr = "number of iterations", required = true, validate = (_ > 0))
        // Iterations refers to number of times we repeat the PageRank computation, each iter. updates ranks, making them converg to more stable values when many iters.
    val partitions = opt[Int](descr = "number of partitions (0 = determine from input)", required = false, default = Some(0))
    val sources = opt[List[Int]](descr = "space-separated source nodes", required = true)
    verify()
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    val src: List[Int] = args.sources().toList  // Source Nodes
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of partitions: " + args.partitions())
    log.info("Numer of iterations: " + args.iterations())
    val conf = new SparkConf().setAppName("PersonalizedPageRank")
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
    val partitioner = new HashPartitioner(partitions)


    // Build Adjacency List

    val adjList = textFile.map(line => {
      val parts = line.split("\\s+")
      val origin = parts(0).toInt
      val dests = if (parts.length > 1) parts.tail.iterator.map(_.toInt).toArray.toIterable
        else Iterable.empty[Int]
      (origin, dests)
    }).partitionBy(partitioner).cache
       // 'toInt':  input file contains numbers, we take parts 1 as origin, then the list of destinations
       // don't .groupByKey as in the file, origins are already grouped by line
       // we partition by key (origin) using partitioner
       // cache saves the RDD in memory, as it will be reused in each iteration !!
    

    val srcSet = src.toSet
    val m = src.size // Amount of source nodes
    val N = adjList.count // Amount of different nodes as origin (# of unique keys are the size of the graph) - # of nodes


    // Initial Ranks (Only Source Nodes)

    var ranks = adjList.filter{case(node,_) => src.contains(node)}.mapValues(_ => 1.0f / m).partitionBy(partitioner).cache
    // Take only nodes in src, assign map Value as 1/m, use same partitioner as adjList, cache


    // 'ranks' is the Adjacency List with the initial rank as value only for each source node
    // 'adjList' is the Adjacency List with the destinations as value for each origin node


    // Compute Ranks

    for (i <- 1 to iterations) {
      val previousRanks = ranks

      val contribs = adjList.join(ranks).values.flatMap{case (adj, rank) =>  // '.values' We take out the key (the origin) of the resultant join list, keeping destinations and rank
          if (adj.nonEmpty){
            val size = adj.size // Amount of destinations per node ('adj' is the list of destinations) 
            adj.map(_ -> rank / size) // For each element in 'adj' distribute the rank equitatively, creating a pair of (dest. node, its rank)
          } else {
            // Dead-End -> send its mass equally to the source nodes -> no missing mass
            src.map(s => (s, rank / m))
          }
      }
      // contribs: list of all nodes that were a destination, with their assigned ranks (flatMap joins all the adj.maps computed into a list of pairs)
    

      // We have to handle nodes with in-degree 0 (they are not a destination for any node)
        // We need to consider ALL NODES, and some nodes may not be have a rank assigned yet, so they wont be in adjList
      val allNodes = adjList.keys.union(adjList.values.flatMap(identity)).distinct().cache


      // Ranks - include also nodes that aren't sources (they aren't in ranks)
        // reduceByKey: combine 'contribs' by key, suming the different ranks assigned to same nodes by different origins

      val sumContribs = contribs.reduceByKey(_ + _, partitions).partitionBy(partitioner)
      val newRanks = allNodes.map(id => (id, 0.0f)).partitionBy(partitioner).leftOuterJoin(sumContribs).mapValues{ case (zero, contrib) => contrib.getOrElse(0.0f)}.cache


      // DAMPING: jumps only go to source nodes
      ranks = newRanks.mapPartitions(iter => {
        iter.map{ case (id, rank) => val jump = if (srcSet.contains(id)) ((1 - DAMPING) / m).toFloat else 0.0f
        (id, rank * DAMPING + jump)
        }
      }, preservesPartitioning = true).cache

      previousRanks.unpersist()
    }
    ranks.saveAsTextFile(args.output())

    adjList.unpersist()
    ranks.unpersist()

    // Get top 20 nodes
    val top20 = ranks.takeOrdered(20)(Ordering.by[(Int, Float), Float](x  => -x._2))

    top20.foreach{ case (id, rank) =>
      println(s"$id\t$rank")
    }

  }
}