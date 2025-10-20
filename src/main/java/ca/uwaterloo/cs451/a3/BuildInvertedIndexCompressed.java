package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
    private static final Text WORD = new Text();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(doc.toString());

      // Build a histogram of the terms.
      COUNTS.clear();
      for (String token : tokens) {
        COUNTS.increment(token);
      }

      // Emit postings
      for (PairOfObjectInt<String> e : COUNTS) {
        WORD.set(e.getLeftElement());
        context.write(WORD, new PairOfInts((int) docno.get(), e.getRightElement()));
      }
    }
  }

  // Reducer: compressed postings as BytesWritable

  // Instead of "PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>" , what is saving postings as ArrayListWritable java objects, they are stored as 
  // compressed bytes arrays "BytesWritable"
  private static final class MyReducer extends Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, BytesWritable>> {
    private static final IntWritable DF = new IntWritable();
    private static final BytesWritable POSTINGS = new BytesWritable();

    @Override
    public void reduce(Text key, Iterable<PairOfInts> values, Context context)
        throws IOException, InterruptedException {
    
      // Buffer postings in memory to sort and apply delta-compression
      List<PairOfInts> postingsList = new ArrayList<>();
      for (PairOfInts p : values) {
        postingsList.add(new PairOfInts(p.getLeftElement(), p.getRightElement()));
      }

      Collections.sort(postingsList);
      int df = postingsList.size();
      DF.set(df);

      // Compress postings: [docid_gap, tf] using VInts
      // ByteArrayOutputStream and DataOutputStream are used to encode gaps (difference between docs IDs) and term frequency tf as VInt
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream dataStream = new DataOutputStream(byteStream);
      int prevDocId = 0;
      for (int i = 0; i < postingsList.size(); i++) {
        int docid = postingsList.get(i).getLeftElement();
        int tf = postingsList.get(i).getRightElement();
        // Generate gaps
        int gap = (i == 0) ? docid : (docid - prevDocId);
        // Write into dataStream (data is stored as binary bytes into byteStream)
        WritableUtils.writeVInt(dataStream, gap);
        WritableUtils.writeVInt(dataStream, tf);
        prevDocId = docid;
      }
      dataStream.close();
      byte[] bytes = byteStream.toByteArray();
      POSTINGS.set(bytes, 0, bytes.length);  // POSTINGS.set(byte[] data, int offset, int length)

      // Result is a pair (df, compressed postings)
      context.write(key, new PairOfWritables<>(DF, POSTINGS));
    }
  }

  private BuildInvertedIndexCompressed() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    int reducers = 1;
  }

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

    LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - reducers: " + args.reducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(args.reducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfInts.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
