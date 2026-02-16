# CS451 - Data-Intensive Distributed Computing Assignments

#### This repository contains my implementation of core distributed data processing patterns using `Hadoop MapReduce` and `Apache Spark`, including batch analysis, page rank, word counting, indexing, graph mining, ML classification and streaming.
Accross the assignments I developed cluster executions (**HDFS** + scheduler constraints), parallelism tuning, scalable algorithm design (pair/stripes, partitioning, compression) and production-minded data workflows (testing on a shared cluster, performance trade-offs and correctness validation) 
> **Note:** This repository contains only the code for the assignments listed. The input data (collections, datasets, graphs) and the original assignments statements are not included. For more details about how to run the code or access the original data, contact me.

### Directory Structure

```text
.
├── .gitignore
├── pom.xml   
├── Assignments_marks/
│   ├── .md files with the grades for my assignments
│
└── src/
    ├── main/java/ca/uwaterloo/cs451/
    │   ├── a0/        → Assignment 0: Java code
    │   ├── a1/        → Assignment 1: Java code
    │   ├── a2/        → Assignment 2: Java code
    │   └── a3/        → Assignment 3: Java code
    └── main/scala/ca/uwaterloo/cs451/
        ├── a2/        → Assignment 2: Scala code
        ├── a4/        → Assignment 4: Scala code
        ├── a5/        → Assignment 5: Scala code
        ├── a6/        → Assignment 6: Scala code + PDFs for Q5
        └── a7/        → Assignment 7: Scala code
```

## Assignments Descriptions

### A0 - Hadoop & Spark Environment Setup

Implemented baseline MapReduce/Sparks jobs (WordCount and document-level term counting) and executed them locally and on the Datasci cluster.  
Focused on configuring **reducers**, understanding parallel task execution, and extracting top-K frequent terms.  

### A1 - Counting in MapReduce

Built bigram relative frequency and co-occurrence matrix style computations in Hadoop MapReduce.  
Implemented Pointwise Mutual Information (**PMI**) over text collections using both Pairs and Stripes approaches.  
  
### A2 - Counting in Spark

Ported Bigram Relative Frequency and PMI implementation from MapReduce to Spark (`Scala`), preserving the algorithmic ideas while using Spark transformations effectively.  
Validated correctness and scalability in a Linux environment and on **HDFS** in a distributed cluster settings.  

### A3 - Inverted Indexing

Implemented an inverted index pipeline with practical IR optimizations: index **compression**, buffering postings and term partitioning.  
Added **Boolean Retrieval** capabilities over the generated index.  

### A4 - PageRank & Multi-Source Personalized PageRank

Implemented PageRank style iterative graph computation, including multi-source personalized variants.  ​
Modeled pages graphs as nodes/edges (Gnutella-style connectivity) and applied iterative convergence-based processing.  

### A5 - Spam Classification

Developed efficient spam filtering and re-ranking worflows in Spark, starting from a basic classifier and extending to an ensemble approach.  
Studied the impact of data shuffling on performance and effectiveness.  
  
### A6 - SQL Data Analytics (RDD-based)  

Handcrafted Spark programs replicating SQL-style analytical **queries** for a data warehousing scenario without using Spark SQL / DataFrame API; operating directly on **raw RDDs**.  
Worked with TPC-H benchmark data, scaling up runs on the Datasci cluster, and read Parquet-based datasets for analytical and visualization outputs. 
  
### A7 - Spark Streaming

Built streaming analytics over a very large taxi-trip dataset (over a billion of individual trips over the past years), implementing **event counters**, **windowed aggregations** (event-time windows), and location based filtering of taxi dropoffs using bounding doxes (having the coordinates as parameters).  
Implemented **trend detection** logic using streaming timestamps and aggregation windows to surface time-varying patterns at scale.
