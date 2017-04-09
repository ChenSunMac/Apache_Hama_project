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
package org.apache.hama.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.commons.util.TextPair;
import org.apache.hama.graph.GraphJob;
import org.junit.Test;

public class BipartiteMatchingTest extends TestCase {

  private String[] input = { "A L:B", "B R:A", "C L:D", "D R:A C" };

  private final static String DELIMETER = "\t";

  private Map<String, String> output1 = new HashMap<String, String>();
  {
    output1.put("A", "B L");
    output1.put("B", "A R");
    output1.put("C", "D L");
    output1.put("D", "C R");
  }

  public static class CustomTextPartitioner implements
      Partitioner<Text, TextPair> {

    @Override
    public int getPartition(Text key, TextPair value, int numTasks) {
      return Character.getNumericValue(key.toString().charAt(0)) % numTasks;
    }

  }

  private static String INPUT = "/tmp/graph.txt";
  private static String OUTPUT = "/tmp/graph-bipartite";

  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  private void generateTestData() {
    FileWriter fout = null;
    BufferedWriter bout = null;
    PrintWriter pout = null;
    try {
      fout = new FileWriter(INPUT);
      bout = new BufferedWriter(fout);
      pout = new PrintWriter(bout);
      for (String line : input) {
        pout.println(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (pout != null) {
          pout.close();
        }
        if (bout != null) {
          bout.close();
        }
        if (fout != null) {
          fout.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private void verifyResult() throws IOException {
    FileStatus[] files = fs.globStatus(new Path(OUTPUT + "/part-*"));
    assertTrue("Not enough files found: " + files.length, files.length == 1);

    for (FileStatus file : files) {
      if (file.getLen() > 0) {
        FSDataInputStream in = fs.open(file.getPath());
        BufferedReader bin = new BufferedReader(new InputStreamReader(in));

        String s = null;
        while ((s = bin.readLine()) != null) {
          String[] lineA = s.split(DELIMETER);
          String expValue = output1.get(lineA[0]);
          assertNotNull(expValue);
          System.out.println(lineA[0] + " -> " + lineA[1] + " expvalue = "
              + expValue);
          assertEquals(expValue, lineA[1]);
        }
        in.close();
      }
    }
  }

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(INPUT)))
        fs.delete(new Path(INPUT), true);
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
      if (fs.exists(new Path("/tmp/partitions")))
        fs.delete(new Path("/tmp/partitions"), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testBipartiteMatching() throws IOException, InterruptedException,
      ClassNotFoundException {
    deleteTempDirs();
    generateTestData();
    try {
      HamaConfiguration conf = new HamaConfiguration();
      GraphJob job = BipartiteMatching.createJob(new String[] { INPUT, OUTPUT,
          "30", "2" }, conf);
      job.setPartitioner(CustomTextPartitioner.class);
      job.setNumBspTask(1);

      long startTime = System.currentTimeMillis();
      if (job.waitForCompletion(true)) {
        System.out.println("Job Finished in "
            + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
      }

      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }
}
