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

import tl.lin.data.map.HMapStIW;
import tl.lin.data.pair.PairOfStrings;

/**
 * <p>
 * Implementation of the "stripes" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 * @ author Lingzi Hong
 */
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

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
  second job 
  */
  private static class Map_Second extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final HMapStIW MAP = new HMapStIW();
    private static final Text KEY = new Text();
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
        MAP.clear();

        for (int j = 0; j < list.size(); j++) {
          //skip itself
          if (j == i)
            continue;

          // skip empty tokens
          String tt=list.get(j);
          if (tt.length() == 0)
            continue;

          MAP.increment(tt);
        }
        KEY.set(word);
        context.write(KEY,MAP);
 //       System.out.println(word);
 //       System.out.println(MAP.toString());
      }           
    }
  }


  protected static class Reduce_Second extends
      Reducer<Text, HMapStIW, PairOfStrings, FloatWritable> {
    private static final FloatWritable VALUE = new FloatWritable(1); //remember to change after debug
    private static final PairOfStrings PAIR = new PairOfStrings();

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
    public void reduce(Text key, Iterable<HMapStIW> values, Context context)
        throws IOException, InterruptedException 
    {
 //     System.out.println("code 1*****************");
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW map = new HMapStIW();
      while (iter.hasNext())
      {
        map.plus(iter.next());
      }

      String ss= map.toString();
 //     System.out.println(ss);
 //     System.out.println(key.toString());
      String subss = ss.substring(1,ss.length()-1);
      String[] kvpairs=subss.split("\\,");
      String kv="";
      float leftwordcnt=0;
      float rightwordcnt=0;

 //     System.out.println("code 2******************"+kvpairs.length);
      int len=kvpairs.length;
      for (int i=0;i<len;i++)
      {
 //         System.out.println("code 3*******************");
          kv=kvpairs[i].trim();
 //         System.out.println(kv);
          String[] kandv=kv.split("\\="); //out of index; use if to see what is the problem
          if (kandv.length>1)
          {
            String t1=kandv[0];
            String t2=kandv[1];
            float v=Float.parseFloat(t2);
 //         System.out.println(t1);
 //         System.out.println(t2);
 //         System.out.println(v);
  
            if (v>9)
            {
              leftwordcnt=singlewordmap.get(key.toString());
              rightwordcnt=singlewordmap.get(t1);
 //           System.out.println(leftwordcnt);
 //           System.out.println(rightwordcnt);
              float respmi=(float)Math.log10(1.0*v/(leftwordcnt*rightwordcnt));
 //           System.out.println(respmi);
 //           System.out.println(key.toString()+"and"+t1);

              PAIR.set(key.toString(),t1);
              VALUE.set(respmi);
              context.write(PAIR,VALUE);
            }
          }
      }
    }
  }


  /**
   * Creates an instance of this tool.
   */



  public StripesPMI() {}

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

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
//    LOG.info(" - window: " + window);
    LOG.info(" - number of reducers: " + reduceTasks);

    //JobConf conf = new JobConf(PairsPMI.class);
    // first job
    //Job job1 = new Job (conf,"join1");
    Configuration conf1 = getConf();
    Job job1 = Job.getInstance(conf1);
    job1.setJobName(StripesPMI.class.getSimpleName());
    job1.setJarByClass(StripesPMI.class);

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
    job2.setJobName(StripesPMI.class.getSimpleName());
    job2.setJarByClass(StripesPMI.class);

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

    job2.setMapOutputKeyClass(Text.class);//map output key   
    job2.setMapOutputValueClass(HMapStIW.class);//map output value   
      
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
    ToolRunner.run(new StripesPMI(), args);
  }
}  
    
