/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hama.bsp;

import com.google.common.base.Preconditions;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.message.MessageManager;
import org.apache.hama.bsp.message.OutgoingMessageManager;
import org.apache.hama.bsp.message.queue.MessageQueue;
import org.apache.hama.graph.*;

import java.io.IOException;

public class YARNGraphJob extends YARNBSPJob {
  public final static String VERTEX_CLASS_ATTR = "hama.graph.vertex.class";
  public final static String VERTEX_ID_CLASS_ATTR = "hama.graph.vertex.id.class";
  public final static String VERTEX_VALUE_CLASS_ATTR = "hama.graph.vertex.value.class";
  public final static String VERTEX_EDGE_VALUE_CLASS_ATTR = "hama.graph.vertex.edge.value.class";

  public final static String VERTEX_OUTPUT_WRITER_CLASS_ATTR = "hama.graph.vertex.output.writer.class";
  public final static String AGGREGATOR_CLASS_ATTR = "hama.graph.aggregator.class";

  public YARNGraphJob(HamaConfiguration conf, Class<?> exampleClass)
      throws IOException {
    super(conf);
    conf.setClass(MessageManager.OUTGOING_MESSAGE_MANAGER_CLASS,
        OutgoingVertexMessageManager.class, OutgoingMessageManager.class);
    conf.setBoolean(Constants.FORCE_SET_BSP_TASKS, true);
    conf.setBoolean(Constants.ENABLE_RUNTIME_PARTITIONING, false);
    conf.setBoolean("hama.use.unsafeserialization", true);

    this.setBspClass(GraphJobRunner.class);
    this.setJarByClass(exampleClass);
    this.setVertexIDClass(Text.class);
    this.setVertexValueClass(IntWritable.class);
    this.setEdgeValueClass(IntWritable.class);
    this.setPartitioner(HashPartitioner.class);
    this.setMessageQueueBehaviour(MessageQueue.PERSISTENT_QUEUE);
  }

    /**
     * Set the Vertex class for the job.
     */

  public void setVertexClass(Class<? extends
      Vertex<? extends Writable, ? extends Writable, ? extends Writable>> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_CLASS_ATTR, cls, Vertex.class);
    setInputKeyClass(cls);
    setInputValueClass(NullWritable.class);
  }

  /**
   * Set the Vertex ID class for the job.
   */
  public void setVertexIDClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_ID_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Vertex value class for the job.
   */
  public void setVertexValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the Edge value class for the job.
   */
  public void setEdgeValueClass(Class<? extends Writable> cls)
      throws IllegalStateException {
    conf.setClass(VERTEX_EDGE_VALUE_CLASS_ATTR, cls, Writable.class);
  }

  /**
   * Set the aggregator for the job.
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void setAggregatorClass(Class<? extends Aggregator> cls) {
    this.setAggregatorClass(new Class[] { cls });
  }

  /**
   * Sets multiple aggregators for the job.
   */
  @SuppressWarnings("rawtypes")
  public void setAggregatorClass(Class<? extends Aggregator>... cls) {
    String classNames = "";
    for (Class<? extends Aggregator> cl : cls) {
      classNames += cl.getName() + ";";
    }
    conf.set(AGGREGATOR_CLASS_ATTR, classNames);
  }

  /**
   * Sets the input reader for parsing the input to vertices.
   */
  public void setVertexInputReaderClass(
      @SuppressWarnings("rawtypes") Class<? extends VertexInputReader> cls) {
    ensureState(JobState.DEFINE);
    conf.setClass(Constants.RUNTIME_PARTITION_RECORDCONVERTER, cls,
        PartitioningRunner.RecordConverter.class);
  }

  /**
   * Sets the output writer for materializing vertices to the output sink. If
   * not set, the default DefaultVertexOutputWriter will be used.
   */
  public void setVertexOutputWriterClass(
      @SuppressWarnings("rawtypes") Class<? extends VertexOutputWriter> cls) {
    ensureState(JobState.DEFINE);
    conf.setClass(VERTEX_OUTPUT_WRITER_CLASS_ATTR, cls,
        VertexOutputWriter.class);
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Vertex<? extends Writable, ? extends Writable, ? extends Writable>> getVertexClass() {
    return (Class<? extends Vertex<? extends Writable, ? extends Writable, ? extends Writable>>) conf
        .getClass(VERTEX_CLASS_ATTR, Vertex.class);
  }

  @Override
  public void setPartitioner(
      @SuppressWarnings("rawtypes") Class<? extends Partitioner> theClass) {
    super.setPartitioner(theClass);
  }

  @Override
  public void setCombinerClass(
      Class<? extends Combiner<? extends Writable>> cls) {
    ensureState(JobState.DEFINE);
    conf.setClass(Constants.COMBINER_CLASS, cls, Combiner.class);
  }

  /**
   * Sets how many iterations the algorithm should perform, -1 for deactivated
   * is default value.
   */
  public void setMaxIteration(int maxIteration) {
    conf.setInt("hama.graph.max.iteration", maxIteration);
  }

  @Override
  public void submit() throws IOException, InterruptedException {
    Preconditions
        .checkArgument(this.getConfiguration().get(VERTEX_CLASS_ATTR) != null,
            "Please provide a vertex class!");
    Preconditions.checkArgument(
        this.getConfiguration().get(VERTEX_ID_CLASS_ATTR) != null,
        "Please provide an vertex ID class!");
    Preconditions.checkArgument(
        this.getConfiguration().get(VERTEX_VALUE_CLASS_ATTR) != null,
        "Please provide an vertex value class, if you don't need one, use NullWritable!");
    Preconditions.checkArgument(
        this.getConfiguration().get(VERTEX_EDGE_VALUE_CLASS_ATTR) != null,
        "Please provide an edge value class, if you don't need one, use NullWritable!");

    Preconditions.checkArgument(
        this.getConfiguration().get(Constants.RUNTIME_PARTITION_RECORDCONVERTER)
            != null,
        "Please provide a converter class for your vertex by using GraphJob#setVertexInputReaderClass!");

    if (this.getConfiguration().get(VERTEX_OUTPUT_WRITER_CLASS_ATTR) == null) {
      this.setVertexOutputWriterClass(DefaultVertexOutputWriter.class);
    }

    this.getConfiguration().setClass(MessageManager.RECEIVE_QUEUE_TYPE_CLASS,
        IncomingVertexMessageManager.class, MessageQueue.class);

    super.submit();
  }
}
