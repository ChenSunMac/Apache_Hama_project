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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.commons.util.TextPair;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

import com.google.common.base.Objects;

/**
 * Randomized Matching Algorithm for bipartite matching in the Pregel Model.
 * 
 */
public final class BipartiteMatching {

  private static final Text UNMATCHED = new Text("U");
  public static final String SEED_CONFIGURATION_KEY = "bipartite.matching.seed";

  public static class BipartiteMatchingVertex extends
      Vertex<Text, NullWritable, TextPair> {

    // Components
    private final static Text LEFT = new Text("L");
    private final static Text RIGHT = new Text("R");

    @Override
    public void setup(HamaConfiguration conf) {
      this.getPeer().getNumCurrentMessages();
    }

    @Override
    public void compute(Iterable<TextPair> msgs) throws IOException {
      if (isMatched()) {
        voteToHalt();
        return;
      }

      switch ((int) getSuperstepCount() % 4) {
        case 0:
          if (Objects.equal(getValue().getSecond(), LEFT)) {
            sendMessageToNeighbors(getNewMessage());
          }
          break;

        case 1:
          if (Objects.equal(getValue().getSecond(), RIGHT)) {
            List<TextPair> buffer = new ArrayList<TextPair>();
            for (TextPair next : msgs) {
              buffer.add(new TextPair(next.getFirst(), next.getSecond()));
            }
            if (buffer.size() > 0) {
              Random random = new Random(System.currentTimeMillis());
              TextPair luckyMsg = buffer.get(RandomUtils.nextInt(random,
                  buffer.size()));

              sendMessage(luckyMsg.getFirst(), getNewMessage());
            }
          }
          break;

        case 2:
          if (Objects.equal(getValue().getSecond(), LEFT)) {
            List<TextPair> buffer = new ArrayList<TextPair>();
            for (TextPair next : msgs) {
              buffer.add(new TextPair(next.getFirst(), next.getSecond()));
            }
            if (buffer.size() > 0) {
              Random random = new Random(System.currentTimeMillis());
              TextPair luckyMsg = buffer.get(RandomUtils.nextInt(random,
                  buffer.size()));

              getValue().setFirst(luckyMsg.getFirst());
              sendMessage(luckyMsg.getFirst(), getNewMessage());
            }
          }
          break;

        case 3:
          if (Objects.equal(getValue().getSecond(), RIGHT)) {
            Iterator<TextPair> messages = msgs.iterator();
            if (messages.hasNext()) {
              TextPair next = messages.next();
              getValue().setFirst(next.getFirst());
            }
          }
          break;
      }
    }

    private TextPair getNewMessage() {
      return new TextPair(new Text(getVertexID()), new Text("1"));
    }

    private boolean isMatched() {
      return !getValue().getFirst().equals(UNMATCHED);
    }

  }

  /**
   * 
   * Input graph is given as<br/>
   * <Vertex> <component value>:<edge 1> <edge 2> ..<br/>
   * A L:B<br/>
   * B R:A<br/>
   * C L:D<br/>
   * D R:A C<br/>
   */
  public static class BipartiteMatchingVertexReader extends
      VertexInputReader<LongWritable, Text, Text, NullWritable, TextPair> {

    @Override
    public boolean parseVertex(LongWritable key, Text value,
        Vertex<Text, NullWritable, TextPair> vertex) throws Exception {

      String[] tokenArray = value.toString().split(":");
      String[] adjArray = tokenArray[1].trim().split(" ");
      String[] selfArray = tokenArray[0].trim().split(" ");

      vertex.setVertexID(new Text(selfArray[0]));
      vertex.setValue(new TextPair(UNMATCHED, new Text(selfArray[1])));
      // initially a node is unmatched, which is denoted by U.

      for (String adjNode : adjArray) {
        vertex.addEdge(new Edge<Text, NullWritable>(new Text(adjNode), null));
      }
      return true;
    }
  }

  private static void printUsage() {
    System.out.println("Usage: <input> <output> "
        + "[maximum iterations (default 30)] [tasks] [seed]");
    System.exit(-1);
  }

  public static GraphJob createJob(String[] args, HamaConfiguration conf)
      throws IOException {
    GraphJob job = new GraphJob(conf, BipartiteMatching.class);

    // set the defaults
    job.setMaxIteration(30);
    job.setNumBspTask(2);

    if (args.length >= 4)
      job.setNumBspTask(Integer.parseInt(args[3]));
    if (args.length >= 3)
      job.setMaxIteration(Integer.parseInt(args[2]));

    job.setJobName("BipartiteMatching");
    job.setInputPath(new Path(args[0]));
    job.setOutputPath(new Path(args[1]));

    job.setVertexClass(BipartiteMatchingVertex.class);
    job.setVertexIDClass(Text.class);
    job.setVertexValueClass(TextPair.class);
    job.setEdgeValueClass(NullWritable.class);

    job.setInputFormat(TextInputFormat.class);
    job.setInputKeyClass(LongWritable.class);
    job.setInputValueClass(Text.class);
    
    job.setVertexInputReaderClass(BipartiteMatchingVertexReader.class);
    job.setPartitioner(HashPartitioner.class);
    
    job.setOutputFormat(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextPair.class);
    return job;
  }

  public static void main(String... args) throws IOException,
      InterruptedException, ClassNotFoundException {

    if (args.length < 2) {
      printUsage();
    }

    HamaConfiguration conf = new HamaConfiguration(new Configuration());

    GraphJob job = createJob(args, conf);

    long startTime = System.currentTimeMillis();
    if (job.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }

}
