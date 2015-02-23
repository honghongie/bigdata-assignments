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

package edu.umd.cloud9.example.ir;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndex.class);
  //use key pairofobjects<VIntWritable，VIntWritable> and value VIntWritable
  private static class MyMapper extends Mapper<LongWritable, Text, PairOfObjects<VIntWritable, VIntWritable>, VIntWritable> {
    private static final PairOfObjects<VIntWritable, VIntWritable> PAIR= new PairOfObjects();
    private static final VIntWritable VALUE = new VIntWritable(1);

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
        PAIR.set(term, docno.get());
        context.write(PAIR, VALUE);
        //special pairs also emit(term,*)
        PAIR.set(term, "*");
        context.write(PAIR, VALUE);

      }
    }
  }

  protected static class MyCombiner extends
      Reducer<PairOfObjects<VIntWritable, VIntWritable>, VIntWritable, PairOfObjects<VIntWritable, VIntWritable>, VIntWritable> {
    private static final VIntWritable SUM = new VIntWritable();

    @Override
    public void reduce(PairOfObjects<VIntWritable, VIntWritable> key, Iterable<VIntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<VIntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static class MyReducer extends
      Reducer<PairOfObjects<VIntWritable, VIntWritable>, VIntWritable, Text, PairOfWritables<VIntWritable, ArrayListWritable<PairOfInts>>> {
    private final static VIntWritable DF = new VIntWritable();
    Text tprev = new Text();
    ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();

    @Override
    public void reduce(PairOfObjects<VIntWritable, VIntWritable> key, Iterable<VIntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfInts> iter = values.iterator();
      // get sum of all values
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next.get();
      }
      // wirte out DF and (docid,frequency) pairs
    
      if (tprev==null && key.getRightElement()=="*"){ //Initialize the first DF; make sure first pair is (word,*) only for the first pair
        DF.set(sum);
        tprev = term;
      }else{
        VIntWritable term = kye.getLeftElement();
        if (term!=tpre && tprev!=null){ //get to a new term and previous one is not null; first pair is (word,*)
          context.write(key,
            new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, postings));
          if (key.getRightElement()=="*"){
            DF.set(sum);
            tprev = term;
          }
        }else{
          int docid = key.getRightElement();
          int tf = sum;
          postings.add(docid,tf);
          tprev = term;
        }
      }
    }
    //reduce method close how to write to context?
    //how to emit last postings?

  }

  protected static class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
    @Override
    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private BuildInvertedIndex() {}

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

    LOG.info("Tool name: " + BuildInvertedIndex.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndex.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndex.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfInts.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

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
    ToolRunner.run(new BuildInvertedIndex(), args);
  }
}
