/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.honghongie;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;
import tl.lin.data.pair.PairOfStringLong;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringLong, IntWritable> {
    private static final PairOfStringLong PAIR= new PairOfStringLong(); //Whether docid can be saved in Intwritable?
    private static final IntWritable VALUE = new IntWritable(1);

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();

      String[] terms = text.split("\\s+");

      // build work docid paris 
      for (String term : terms) {
        if (term == null || term.length() == 0) {
          continue;
        }
        long docid = docno.get();
        PAIR.set(term, docid);
        context.write(PAIR, VALUE);

        //special pairs also emit(term,*) //
      //  PAIR.set(term, "*");
      //  context.write(PAIR, VALUE);
      //  System.out.println(PAIR);
      //  System.out.println(VALUE);

      }
    }
  }


  protected static class MyCombiner extends
      Reducer<PairOfStringLong, IntWritable, PairOfStringLong, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(PairOfStringLong key, Iterable<IntWritable> values, Context context)
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

  private static class MyReducer extends
      Reducer<PairOfStringLong, IntWritable, Text, PairOfWritables<IntWritable, ArrayListWritable<VIntWritable>>> {
    private final static IntWritable DF = new IntWritable();
    private final static Text WORD = new Text();
    //
    String tprev = null;
    ArrayListWritable<VIntWritable> postings = new ArrayListWritable<VIntWritable>();
    Long gap = 0L; 

//Initialize using setup?
//    @Override
//    protected void setup(Context context)throws IOException, InterruptedException{
//      String tprev = new String();
//      ArrayListWritable<VIntWritable> postings = new ArrayListWritable<VIntWritable>();
//    }
//

    @Override
    public void reduce(PairOfStringLong key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      // get sum of all values
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      //ensure ascending, if use string, will becomes descending
//      System.out.println("==============key:"+key);
//      System.out.println("==============value:"+sum);
      
      // wirte out DF and (docid,frequency) pairs   
      VIntWritable docid = new VIntWritable();
      VIntWritable tf = new VIntWritable();
   
      String term = key.getLeftElement();

//      if (!term.equals(tprev))System.out.println("term not equal to tprev");
//      if (tprev!= null)System.out.println("tprev not null");

      if (!term.equals(tprev) && tprev!=null){ //get to a new term and previous one is not null; first pair is (word,*)
        WORD.set(tprev);
        DF.set(postings.size()/2);
//        System.out.println("*************DF:"+DF);

        context.write(WORD,
          new PairOfWritables<IntWritable, ArrayListWritable<VIntWritable>>(DF, postings));
//        System.out.println("**************WORD:"+WORD);
//        System.out.println("**************postings:"+postings);
        postings.clear();
        gap = 0L; //clear gap of previous list
      }
      // no matter whether new key, emit the information
      long docnum = key.getRightElement();
      long docnoc = docnum - gap;
      //convert to int so that can be converted to VInt
      String sdocnoc = Long.toString(docnoc);
      int i = Integer.parseInt(sdocnoc);

      docid.set(i);
      tf.set(sum);
      postings.add(docid);
      postings.add(tf);
      tprev = term;
      gap = docnum;
//      System.out.println("*****************"+tprev);
//      System.out.println("docid: "+docid);

    }
  
//emit last posting-----leave this alone to see whether it works 2.25
     @Override
    protected void cleanup(Context context)throws IOException, InterruptedException{
      WORD.set(tprev);
      context.write(WORD,
        new PairOfWritables<IntWritable, ArrayListWritable<VIntWritable>>(DF, postings));
    }
  }

//partitioner according to indexed word
  protected static class MyPartitioner extends Partitioner<PairOfStringLong, IntWritable> {
    @Override
    public int getPartition(PairOfStringLong key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private BuildInvertedIndexCompressed() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + BuildInvertedIndexCompressed.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexCompressed.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(PairOfStringLong.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class); //why mapfileoutputformat?
//    job.setOutputFormatClass(SequenceFileOutputFormat);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
