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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfWritables;

public class BuildInvertedIndexHBase extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexHBase.class);

  //column family name
  public static final String[] FAMILIES = { "p" };
  public static final byte[] CF = FAMILIES[0].getBytes();

  private static class MyMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

    //use previous structure for counting
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();
      COUNTS.clear();
      byte[] DOC = Bytes.toBytes(docno.get()); // get document number

      String[] terms = text.split("\\s+");

      // First build a histogram of the terms.
      for (String term : terms) {
        if (term == null || term.length() == 0) {
          continue;
        }

        COUNTS.increment(term);
      }

      // build HBase table
      for (PairOfObjectInt<String> e : COUNTS) {
        String word = e.getLeftElement();
        Put put = new Put(Bytes.toBytes(word));
        byte[] tf = Bytes.toBytes(e.getRightElement());
        put.addColumn(CF, DOC, tf)
      }
      context.write(new ImmutableBytesWritable(Bytes.toBytes(word)), put)
    }
  }


  private BuildInvertedIndexHBase() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";


  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("table").hasArg()
        .withDescription("HBase table name").create(OUTPUT));
    
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
    String outputTable = cmdline.getOptionValue(OUTPUT);
    
    // If the table doesn't exist, create it
    Configuration conf = getConf();
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    HBaseAdmin admin = new HBaseAdmin(hbaseConfig);

    if (admin.tableExists(outputTable)){
      LOG.info(String.format("Table '%s' exists: dropping table and recreating.", outputTable));
      LOG.info(String.format("Disabling table '%s'", outputTable));
      admin.disableTable(outputTable);
      LOG.info(String.format("Droppping table '%s'", outputTable));
      admin.deleteTable(outputTable);
    }

    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(outputTable));
    for (int i = 0; i < FAMILIES.length; i++) {
      HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
      tableDesc.addFamily(hColumnDesc);
    }
    admin.createTable(tableDesc);
    LOG.info(String.format("Successfully created table '%s'", outputTable));

    admin.close();


    // ready to start running mapreduce

    LOG.info("Tool name: " + BuildInvertedIndexHBase.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output table: " + outputPath);

    Job job = Job.getInstance(conf);
    job.setJobName(BuildInvertedIndexHBase.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexHBase.class);

    Job.setMapperClass(MyMapper.class);

//
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);

//set output table
    job.setOutputFormatClass(TableOutPutFormat.class);
    job.getConfiguration().set(TableOutPutFormat.OUTPUT_TABLE, outputTable);



//input path
    FileInputFormat.setInputPaths(job, new Path(inputPath));


    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexHBase(), args);
  }
}
