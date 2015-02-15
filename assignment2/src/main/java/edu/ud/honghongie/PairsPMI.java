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
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.net.URI;
import java.io.File;
 
import org.apache.commons.io.FileUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfStrings;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 * @ author Lingzi Hong
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  private static class Map_First extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Text WORD = new Text();
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException 
    {
      String line = value.toString();
      String[] terms = line.split("\\s+");
      //get unique set of the line
      ArrayList<String> list = new ArrayList<String>();
      for (int i = 0; i<terms.length; i++)
      {
        if(!list.contains(terms[i]))
        {
          list.add(terms[i]);
        }
      }

      for (int i = 0; i < list.size(); i++) 
      {
        String word = list.get(i);

        // skip empty tokens
        if (word.length() == 0)
          continue;
        WORD.set(word);
        context.write(WORD,ONE);
      }
    }
  }

// output of mapper should be exactly the same with input of combiner
  protected static class MyCombiner extends
      Reducer<Text, IntWritable, Text, IntWritable> {
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
      context.write(key, SUM);
    }
  }

  protected static class Reduce_First extends
      Reducer<Text, IntWritable, Text, IntWritable> {
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
      context.write(key,SUM);
    }
  }


  /*
  second job to join same pairs
  */
  private static class Map_Second extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final FloatWritable ONE = new FloatWritable(1);
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException 
    {
      String line = value.toString(); 
      String[] terms = line.split("\\s+");
      ArrayList<String> list = new ArrayList<String>();
      for (int i = 0; i<terms.length; i++){
        if(!list.contains(terms[i]))
        {
          list.add(terms[i]);
        }
      }
      for (int i = 0; i < list.size(); i++) 
      {
        String word = list.get(i);

        // skip empty tokens
        if (word.length() == 0)
          continue;

        for (int j = 0; j < list.size(); j++) {
          //skip itself
          if (j == i)
            continue;

          // skip empty tokens
          String tt=list.get(j);
          if (tt.length() == 0)
            continue;

          PAIR.set(word, tt);
          context.write(PAIR, ONE);
        }
      }           
    }
  }


  protected static class Reduce_Second extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable VALUE = new FloatWritable();
    Map<String,Float> singlewordmap=new HashMap<String,Float>();//store values for single map
    @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
        URI mappingFileUri = context.getCacheFiles()[0];
        if(mappingFileUri!=null)
        {
     //     System.out.println("mappingFileUri is not null*********");
          String filetext=FileUtils.readFileToString(new File("part-r-00000"));//note the path of the file
          String[] words=filetext.split("\\s+");
     //     System.out.println(words[0]+"*********"+words[1]);
          int cnt=0,len=words.length;
          String word;
          float wordcnt;
          while (cnt+1<len)
          {
            word=words[cnt].trim();//remove space at the start or the end position
     //       System.out.println("*********"+words[cnt+1]);
            wordcnt=Float.parseFloat(words[cnt+1].trim());
            singlewordmap.put(word,wordcnt);
            cnt+=2;
          }
        }      
      }else{
        System.out.println(">>>>>> NO CACHE FILES AT ALL");
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException 
    {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()){
        sum += iter.next().get();
      }
//      System.out.println(sum);
      String leftword=key.getLeftElement();
      String rightword=key.getRightElement();
      float leftwordcnt=0;
      float rightwordcnt=0;
//      System.out.println(leftword+"**************"+rightword+"*****sum is "+sum);
      if (singlewordmap.containsKey(leftword))
         leftwordcnt=singlewordmap.get(leftword);
       if (singlewordmap.containsKey(rightword))
         rightwordcnt=singlewordmap.get(rightword);
       if (leftwordcnt*rightwordcnt==0)
          System.out.println("one word may not be there");
      if (sum>9)
      {
        float respmi=(float)Math.log10(1.0*sum/(leftwordcnt*rightwordcnt));
        VALUE.set(respmi);
        context.write(key, VALUE);
      }

    }
  }

  /**
   * Creates an instance of this tool.
   */



  public PairsPMI() {}

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
   // options.addOption(OptionBuilder.withArgName("num").hasArg()
   //     .withDescription("window size").create(WINDOW));
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
//    int window = cmdline.hasOption(WINDOW) ? 
//        Integer.parseInt(cmdline.getOptionValue(WINDOW)) : 2;

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
//    LOG.info(" - window: " + window);
    LOG.info(" - number of reducers: " + reduceTasks);

    //JobConf conf = new JobConf(PairsPMI.class);
    // first job
    //Job job1 = new Job (conf,"join1");
    Configuration conf1 = getConf();
    Job job1 = Job.getInstance(conf1);
    job1.setJobName(PairsPMI.class.getSimpleName());
    job1.setJarByClass(PairsPMI.class);

    job1.setNumReduceTasks(reduceTasks);
    

    //file path of job1  
    // Delete the output directory if it exist
    Path dir = new Path ("temp");
    FileSystem.get(getConf()).delete(dir,true);

    FileInputFormat.setInputPaths(job1, new Path(inputPath));
    FileOutputFormat.setOutputPath(job1, new Path("temp")); 


    job1.setMapperClass(Map_First.class);
    job1.setCombinerClass(MyCombiner.class);
    job1.setReducerClass(Reduce_First.class);

    job1.setMapOutputKeyClass(Text.class);//map output key   
    job1.setMapOutputValueClass(IntWritable.class);//map output value   
      
    job1.setOutputKeyClass(Text.class);//reduce output key   
    job1.setOutputValueClass(IntWritable.class);//reduce output value   
             
   // ControlledJob ctrljob1=new  ControlledJob(conf);   
   // ctrljob1.setJob(job1);
    
    long startTime1 = System.currentTimeMillis();
    job1.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime1) / 1000.0 + " seconds");

    //begin job2
    //Configuration conf2 = getConf();
    Job job2 = Job.getInstance(getConf());
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);

    job2.setNumReduceTasks(reduceTasks);
    
    //delete the output directory if it exists.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir,true);

    //file path of job2  
    FileInputFormat.setInputPaths(job2, new Path(inputPath));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath));
    job2.addCacheFile(new URI("temp/part-r-00000"));     

    job2.setMapperClass(Map_Second.class);
    job2.setReducerClass(Reduce_Second.class);

    job2.setMapOutputKeyClass(PairOfStrings.class);//map output key   
    job2.setMapOutputValueClass(FloatWritable.class);//map output value   
      
    job2.setOutputKeyClass(PairOfStrings.class);//reduce output key   
    job2.setOutputValueClass(FloatWritable.class);//reduce output value   
             

    long startTime2 = System.currentTimeMillis();
    job2.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime2) / 1000.0 + " seconds");
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}  
    
