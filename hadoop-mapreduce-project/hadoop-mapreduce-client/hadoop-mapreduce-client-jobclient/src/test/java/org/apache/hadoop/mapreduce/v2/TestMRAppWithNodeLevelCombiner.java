/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.v2;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.CustomOutputCommitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import junit.framework.TestCase;

@SuppressWarnings("deprecation")
public class TestMRAppWithNodeLevelCombiner {

  protected static MiniDistributedMRYarnCluster mrCluster;
  protected static MiniDFSCluster dfsCluster;
  private static Configuration conf = new Configuration();
  private static FileSystem localFs;
  private static FileSystem remoteFs;
  private static final Log LOG = LogFactory.getLog(TestMRAppWithCombiner.class);
  private static final int NUM_HADOOP_DATA_NODES = 1;
  
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException io) {
      throw new RuntimeException("problem getting local fs", io);
    }   
    try {
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(NUM_HADOOP_DATA_NODES)
        .format(true).racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }   
  }
  
  private static Path TEST_ROOT_DIR = new Path("target",
      TestMRJobs.class.getName() + "-tmpDir").makeQualified(localFs);
  static Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");

  @BeforeClass
  public static void setup() throws IOException {
    if (!(new File(MiniMRYarnCluster.APPJAR)).exists()) {
      LOG.info("MRAppJar " + MiniMRYarnCluster.APPJAR
          + " not found. Not running test.");
      return;
    }

    if (mrCluster == null) {
      mrCluster = new MiniDistributedMRYarnCluster(TestMRJobs.class.getName(), 3);
      Configuration conf = new Configuration();
      mrCluster.init(conf);
      mrCluster.start();
    }

    // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
    // workaround the absent public discache.
    //localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
    //localFs.setPermission(APP_JAR, new FsPermission("700"));
    
  }

  @AfterClass
  public static void tearDown() {
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
    
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  @Test
  public void testCombinerShouldUpdateTheReporter() throws Exception {
    JobConf conf = new JobConf(mrCluster.getConfig());
    int numMaps = 5;
    int numReds = 1;
    Path in = new Path(remoteFs.getUri().toString() + Path.SEPARATOR +
        "testCombinerShouldUpdateTheReporter-in");
    Path out = new Path(remoteFs.getUri().toString() +Path.SEPARATOR +
        "testCombinerShouldUpdateTheReporter-out");
    System.out.println(in+ "," + out);
    LOG.info("[OZA] creating input and output folder");
    createInputOutputFolder(in, out, numMaps);
    conf.setJobName("test-wordcount-with-node-level-combiner");
    
    Job job = Job.getInstance(conf);
    
    job.setJarByClass(TestMRAppWithNodeLevelCombiner.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    conf.setOutputCommitter(CustomOutputCommitter.class);
    conf.setInputFormat(TextInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    conf.setNodeLevelAggregation(true);
    conf.setNodeLevelAggregationThreshold(2);

    FileInputFormat.addInputPath(conf, in);
    FileOutputFormat.setOutputPath(conf, out);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReds);
    
    //runJob(conf);
    //verifyOutput(conf, out, numMaps, numReds);
  }

  private void verifyOutput(JobConf conf2, Path out, int numMaps, int numOutput) {
    
    for (int i = 0; i < numOutput; ++i) {
      FSDataInputStream file;
      String record;
      long numValidRecords = 0;

      try {
        file = remoteFs.open(out);
        LOG.info("[OZA]"+file);
        while((record = file.readLine()) != null) {
          int blankPos = record.indexOf(" ");
          String keyString = record.substring(0, blankPos);
          String valueString = record.substring(blankPos+1);
          LOG.info("[OZA]"+keyString + valueString);
          // Check for sorted output and correctness of record.
          /*
          if (keyString.compareTo(prevKeyValue) >= 0
              && keyString.equals(valueString)) {
          } else {
            numInvalidRecords++;
          }  
          */
          numValidRecords++;
        }
        
        file.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      Assert.assertEquals(3, numValidRecords);
    }
  }

  static void createInputOutputFolder(Path inDir, Path outDir, int numMaps)
      throws Exception {
    FileSystem fs = remoteFs;
    /*
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    */
    
    String input = "0\n"+ "1 2 2\n" + "0\n";
    for (int i = 0; i < numMaps; ++i) {
      DataOutputStream file = fs.create(new Path(inDir, "part-" + i));
      file.writeBytes(input);
      file.close();
      LOG.info("[OZA]" + new Path(inDir, "part-" + i));
    }
  }

  static boolean runJob(JobConf conf) throws Exception {
    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);
    return jobClient.monitorAndPrintJob(conf, job);
  }

  class MyCombinerToCheckReporter<K, V> extends IdentityReducer<K, V> {
    public void reduce(K key, Iterator<V> values, OutputCollector<K, V> output,
        Reporter reporter) throws IOException {
      if (Reporter.NULL == reporter) {
        Assert.fail("A valid Reporter should have been used but, Reporter.NULL is used");
      }
    }
  }
  
  public static class TokenizerMapper 
  extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static class IntSumReducer 
  extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
        Context context
        ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

}
