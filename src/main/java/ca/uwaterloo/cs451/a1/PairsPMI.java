package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;

import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.fs.FileStatus;

import java.util.Map;
import java.util.HashMap; 
import java.util.Set;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ArrayList;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/**
 * PairsPMI: implementation of the algorith for computing PMI and count of co-occurring pairs
 *      1st Job: counts unique words per line, which will be needed as p(x) for PMI computation. Also counts lines (N)
 * @author Cesar Alvarez-Cascos
 */

public class PairsPMI extends Configured implements Tool {
     private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  // 1st Job: counts unique words per line and number of lines with a counter

  private static final class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Text WORD = new Text();
    private static final IntWritable ONE = new IntWritable(1);
    private int maxWords = 40;
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // Increment Counter of Lines (also empty lines) N
      context.getCounter("PMI", "N").increment(1);
      // Tokenize line (key is the number of the line and value the text in it)
      List<String> tokens = Tokenizer.tokenize(value.toString());
      // If line is empty, nothign has to be done in this one, but counter has already been incremented
        if (tokens == null || tokens.size() == 0) return; 

      // Keep only the first 40 words 
      int limit = Math.min(maxWords, tokens.size());
      // We don't want to count repeated words more than one -> take unique tokens
      Set<String> uniqueW = new HashSet<>();
      for (int i = 0; i < limit; i++) uniqueW.add(tokens.get(i)); // Each unique word one per line
      // Mapper output is the pair: word and its counter/frequency (in fact it is word and ONE, counter will be sum up later)

      for (String w : uniqueW) {
        WORD.set(w);
        context.write(WORD, ONE);
      }
    }
  }

    // Reducer: Aggregate every occurrence of each word 
  private static final class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM); // Pair (word, count)
    }
  }



  // 2nd Job: count co-occurring pairs

  private static final class PairsMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);
    private int maxWords = 40;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // Tokenize each line     
      List<String> tokens = Tokenizer.tokenize(value.toString());
      int limit = Math.min(maxWords, tokens.size());
      // Only unique words, maximum 40 per line
      List<String> uniqueW = new ArrayList<>(new LinkedHashSet<>(tokens.subList(0, limit)));

      for (int i = 0; i < uniqueW.size(); i++) {
        String a = uniqueW.get(i);
        for (int j = 0; j < uniqueW.size(); j++) {
          if (i == j) continue; // Don't count co-occurrence with the word itself
          String b = uniqueW.get(j);
          PAIR.set(a, b);
          context.write(PAIR, ONE); // Create the pair of the co-occurrence
                // (a,b) and (b,a) will be both generated, but PMI is symmetric
        }
      }
    }
  }

  // Combiner: sum counts of pairs
  private static final class PairsCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // Partitioner: chooses which reducer to send the key.
  // In this case send all pairs with same left element (1st key) to same reducer
  private static final class LeftPartitioner extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  // Reducer: compute PMI for each pair of words using the frequencies of 1st Job and applying threshold
  // Out Value of the reducer is Text beacuse we want PMI value and Count of the pair
  private static final class PairsReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, Text> {
    private Map<String, Integer> wordCounts = new HashMap<>(); // For getting the individual words counts
    private long N = 0L; // N : Total number of lines
    private int threshold = 1; // Minimum threshold

    // SET UP & Get the configuration each time reducer is run
    @Override
    protected void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      threshold = conf.getInt("threshold", 1);
      N = conf.getLong("N", 0L);

      // Read wordcounts from Job 1 output (path is 'outputWordCounts')
        // The way this works, is in the job2 instance creation we set its configuration:
        // "conf2.set("wordCountsPath", outputWordCounts.toString())" being "outputWordCounts"
        // the output path of job1. So here we use wcDir as the directory of the job1 output, 
        // which is in the configuration

      Path wcDir = new Path(conf.get("wordCountsPath"));
      FileSystem fs = FileSystem.get(conf);

      for (FileStatus status : fs.listStatus(wcDir)) {
        Path path = status.getPath();
        String pathName = path.getName();
        if (!pathName.startsWith("part-")) continue; // The file we are looking for (reducer output) should start like that

        try (LineReader reader = new LineReader(fs.open(path))) {
          Text txt = new Text(); // This object is like the buffer for the reader for each line
          while (reader.readLine(txt) > 0) {
            String line = txt.toString().trim();
            if (line.isEmpty()) continue;

            String[] values = line.split("\t");
            // List of values that we got from the line of the Job 1 output should be at least 2 (word and count) 
            if (values.length >= 2) {
              String w = values[0];
              int count = Integer.parseInt(values[1]);
              wordCounts.put(w, count);
            }
          }
        }
      }
      // reader.close();
    }

    // Reducer: computes PMI for each words pair
    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      // Threshold: ignore pairs under the defined threshold
      if (sum < threshold) return; 

      String a = key.getLeftElement();
      String b = key.getRightElement();
    
      // Take count of each word (previously obtained in the map built with Job1 output)
      Integer ca = wordCounts.get(a);
      Integer cb = wordCounts.get(b);


      // Avoid errors in PMI computation
      if (ca == null || cb == null || ca == 0 || cb == 0 || N == 0L) {
        return;
      }

      // Compute PMI (sum is p(a,b))
      double numerator = (double) sum * (double) N;
      double denominator = (double) ca.intValue() * (double) cb.intValue();
      double pmi = Math.log10(numerator / denominator);

      // Output: (PMI,count)
      String value1 = String.format("(%f, %d)", pmi, sum);
      context.write(new PairOfStrings(a, b), new Text(value1));
    }
  }

  
  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    // Number of reducers to use (default is 1)
    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    // Threshold: minimum co-occurrences of a pair (default is 1)
    @Option(name = "-threshold", metaVar = "[num]", usage = "cooccurrence threshold")
    int threshold = 1;
  }


  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));
    
    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    // Job 1: words counts - - - - - 
    Job job1 = Job.getInstance(getConf(), "PairsPMI-WordCount");
    job1.setJarByClass(PairsPMI.class);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    job1.setMapperClass(WordCountMapper.class);
    job1.setReducerClass(WordCountReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    // Definte input and output paths of this Job
      // outputWordCounts is the path for the temporary data we need from Job 1 to Job 2
    FileInputFormat.setInputPaths(job1, new Path(args.input));

    Path outputWordCounts = new Path(args.output + "-tmp-c5alvare");
    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(outputWordCounts, true);

    FileOutputFormat.setOutputPath(job1, outputWordCounts); // This is not 'args.output'


    job1.setNumReduceTasks(args.numReducers);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    

    // Get N from counter
    long N = job1.getCounters().findCounter("PMI", "N").getValue();

    // Job 2: pairs counting & PMI computation - - - - - -

    Configuration conf2 = new Configuration(getConf());
    conf2.setInt("threshold", args.threshold);
    conf2.setLong("N", N);
    // Add WordCounts path to the configuration of Job 2
    conf2.set("wordCountsPath", outputWordCounts.toString());


    Job job2 = Job.getInstance(conf2, "PairsPMI-Pairs");
    job2.setJarByClass(PairsPMI.class);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    job2.setMapperClass(PairsMapper.class);
    job2.setCombinerClass(PairsCombiner.class);
    job2.setPartitionerClass(LeftPartitioner.class);
    job2.setReducerClass(PairsReducer.class);

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(Text.class);

    job2.setOutputFormatClass(TextOutputFormat.class);


    // Definte input and output paths of this Job
    FileInputFormat.setInputPaths(job2, new Path(args.input));

    Path outputDir = new Path(args.output);
    // Delete the output directory if it exists already.
    FileSystem.get(conf2).delete(outputDir, true);
    FileOutputFormat.setOutputPath(job2, outputDir);

    job2.setNumReduceTasks(args.numReducers);


    job2.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    return 0;
  }

    /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters any exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
