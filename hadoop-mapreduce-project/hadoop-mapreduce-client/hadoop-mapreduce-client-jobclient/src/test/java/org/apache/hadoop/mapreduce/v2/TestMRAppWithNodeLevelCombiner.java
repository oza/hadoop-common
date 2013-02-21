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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with multiple directories
 * and check for correct classpath
 */
public class TestMRAppWithNodeLevelCombiner {
  MiniDFSCluster dfs = null;
  MiniDistributedMRYarnCluster mrCluster = null;
  FileSystem localFs = null;
  FileSystem remoteFs = null;

  
  void configureWordCount(FileSystem fs, JobConf conf, String input,
      int numMaps, int numReduces, Path inDir, Path outDir) throws IOException {
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    
    for (int i = 0; i < numMaps; i++) {
      DataOutputStream file = fs.create(new Path(inDir, "part-" + Integer.toString(i)));
      file.writeBytes(input);
      file.close();
    }
    FileSystem.setDefaultUri(conf, fs.getUri());
    conf.set(JTConfig.FRAMEWORK_NAME, JTConfig.YARN_FRAMEWORK_NAME);
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.set("mapred.mapper.class", "testjar.ClassWordCount$MapClass");
    conf.set("mapred.combine.class", "testjar.ClassWordCount$Reduce");
    conf.set("mapred.reducer.class", "testjar.ClassWordCount$Reduce");
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    //set the tests jar file
    conf.setJarByClass(TestMRAppWithNodeLevelCombiner.class);
  }

  String launchWordCount(URI fileSys, JobConf conf, String input,
      int numMaps, int numReduces)
          throws IOException {
    final Path inDir = new Path("/testing/wc/input");
    final Path outDir = new Path("/testing/wc/output");
    FileSystem fs = FileSystem.get(fileSys, conf);
    configureWordCount(fs, conf, input, numMaps, numReduces, inDir, outDir);
    JobClient.runJob(conf);
    StringBuffer result = new StringBuffer();
    {
      Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
          new Utils.OutputFileUtils.OutputFilesFilter()));
      for(int i=0; i < fileList.length; ++i) {
        BufferedReader file =
            new BufferedReader(new InputStreamReader(fs.open(fileList[i])));
        String line = file.readLine();
        while (line != null) {
          result.append(line);
          result.append("\n");
          line = file.readLine();
        }
        file.close();
      }
    }
    return result.toString();
  }
  
  @Test(timeout = 80000)
  public void testNodeLevelCombinerWithSingleReducer()
      throws IOException {
    final int numMaps = 5;
    final int numReds = 1;

    try {
      Configuration conf = new Configuration();
      localFs = FileSystem.getLocal(conf);
      dfs = new MiniDFSCluster.Builder(conf).numDataNodes(2)
          .format(true).racks(null).build();
      remoteFs = dfs.getFileSystem();
      mrCluster = new MiniDistributedMRYarnCluster(TestMRAppWithNodeLevelCombiner.class.getName(), 3);
      conf.set("fs.defaultFS", remoteFs.getUri().toString());   // use HDFS
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      conf.setBoolean(MRJobConfig.MAP_NODE_LEVEL_AGGREGATION_ENABLED, true);
      conf.setInt(MRJobConfig.MAP_NODE_LEVEL_AGGREGATION_THRESHOLD, 2);
      mrCluster.init(conf);
      mrCluster.start();
      Path TEST_ROOT_DIR = new Path("target",
          TestMRJobs.class.getName() + "-tmpDir")
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
      Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");
      // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
      // workaround the absent public discache.
      localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
      localFs.setPermission(APP_JAR, new FsPermission("700"));
      
      JobConf jobConf = new JobConf(mrCluster.getConfig());

      String result;
      result = launchWordCount(remoteFs.getUri(), jobConf,
          "The quick brown fox\nhas many silly\n" + "red fox sox\n", numMaps, numReds);
      Assert.assertEquals("The\t" + Integer.toString(1*numMaps) + "\n"
          + "brown\t"  + Integer.toString(1*numMaps) + "\n"
          + "fox\t" + Integer.toString(2*numMaps) + "\n"
          + "has\t" + Integer.toString(1*numMaps) + "\n"
          + "many\t" + Integer.toString(1*numMaps) + "\n"
          + "quick\t" + Integer.toString(1*numMaps) + "\n"
          + "red\t" + Integer.toString(1*numMaps) + "\n"
          + "silly\t"+ Integer.toString(1*numMaps) + "\n" 
          + "sox\t" + Integer.toString(1*numMaps) + "\n", result);

    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mrCluster != null) { mrCluster.stop(); }
    }
  }
  
  @Test(timeout = 80000)
  public void testNodeLevelCombinerWithMultipleReducers()
      throws IOException {
    final int numMaps = 5;
    final int numReds = 3;
    
    try {
      Configuration conf = new Configuration();
      localFs = FileSystem.getLocal(conf);
      dfs = new MiniDFSCluster.Builder(conf).numDataNodes(2)
          .format(true).racks(null).build();
      remoteFs = dfs.getFileSystem();
      mrCluster = new MiniDistributedMRYarnCluster(TestMRAppWithNodeLevelCombiner.class.getName(), 3);
      conf.set("fs.defaultFS", remoteFs.getUri().toString());   // use HDFS
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, "/apps_staging_dir");
      conf.setBoolean(MRJobConfig.MAP_NODE_LEVEL_AGGREGATION_ENABLED, true);
      conf.setInt(MRJobConfig.MAP_NODE_LEVEL_AGGREGATION_THRESHOLD, 2);
      mrCluster.init(conf);
      mrCluster.start();
      Path TEST_ROOT_DIR = new Path("target",
          TestMRJobs.class.getName() + "-tmpDir")
          .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
      Path APP_JAR = new Path(TEST_ROOT_DIR, "MRAppJar.jar");
      // Copy MRAppJar and make it private. TODO: FIXME. This is a hack to
      // workaround the absent public discache.
      localFs.copyFromLocalFile(new Path(MiniMRYarnCluster.APPJAR), APP_JAR);
      localFs.setPermission(APP_JAR, new FsPermission("700"));
      
      JobConf jobConf = new JobConf(mrCluster.getConfig());

      String result;
      result = launchWordCount(remoteFs.getUri(), jobConf,
          "The quick brown fox\nhas many silly\n" + "red fox sox\n", numMaps, numReds);
      Assert.assertEquals("many\t" + Integer.toString(1*numMaps) + "\n"
          + "silly\t"  + Integer.toString(1*numMaps) + "\n"
          + "brown\t" + Integer.toString(1*numMaps) + "\n"
          + "fox\t" + Integer.toString(2*numMaps) + "\n"
          + "red\t" + Integer.toString(1*numMaps) + "\n"
          + "The\t" + Integer.toString(1*numMaps) + "\n"
          + "has\t" + Integer.toString(1*numMaps) + "\n"
          + "quick\t"+ Integer.toString(1*numMaps) + "\n" 
          + "sox\t" + Integer.toString(1*numMaps) + "\n", result);

    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mrCluster != null) { mrCluster.stop(); }
    }
  }
  
}
