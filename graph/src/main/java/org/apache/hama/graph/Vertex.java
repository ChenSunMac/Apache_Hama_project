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
package org.apache.hama.graph;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Counters.Counter;

/**
 * Vertex is a abstract definition of Google Pregel Vertex. For implementing a
 * graph application, one must implement a sub-class of Vertex and define, the
 * message passing and message processing for each vertex.
 * 
 * Every vertex should be assigned an ID. This ID object should obey the
 * equals-hashcode contract and would be used for partitioning.
 * 
 * The edges for a vertex could be accessed and modified using the
 * {@link Vertex#getEdges()} call. The self value of the vertex could be changed
 * by {@link Vertex#setValue(Writable)}.
 * 
 * @param <V> Vertex ID object type
 * @param <E> Edge cost object type
 * @param <M> Vertex value object type
 */
@SuppressWarnings("rawtypes")
public abstract class Vertex<V extends WritableComparable, E extends Writable, M extends Writable>
    implements VertexInterface<V, E, M> {

  private transient GraphJobRunner<V, E, M> runner;

  private V vertexID;
  private M oldValue;
  private M value;
  private List<Edge<V, E>> edges;

  private boolean votedToHalt = false;
  private long lastComputedSuperstep = 0;

  public HamaConfiguration getConf() {
    return runner.getPeer().getConfiguration();
  }

  public Vertex() {
  }

  @Override
  public V getVertexID() {
    return this.vertexID;
  }

  @Override
  public void setup(HamaConfiguration conf) {
  }

  @Override
  public void sendMessage(Edge<V, E> e, M msg) throws IOException {
    runner.sendMessage(e.getDestinationVertexID(), msg);
  }

  @Override
  public void sendMessage(V destinationVertexID, M msg) throws IOException {
    runner.sendMessage(destinationVertexID, msg);
  }

  @Override
  public void sendMessageToNeighbors(M msg) throws IOException {
    runner.sendMessage(this.getEdges(), msg);
  }

  private void alterVertexCounter(int i) throws IOException {
    this.runner.setChangedVertexCnt(this.runner.getChangedVertexCnt() + i);
  }

  @Override
  public void addVertex(V vertexID, List<Edge<V, E>> edges, M value)
      throws IOException {
    MapWritable msg = new MapWritable();
    // Create the new vertex.
    Vertex<V, E, M> vertex = GraphJobRunner
        .<V, E, M> newVertexInstance(GraphJobRunner.VERTEX_CLASS);
    vertex.setEdges(edges);
    vertex.setValue(value);
    vertex.setVertexID(vertexID);

    msg.put(GraphJobRunner.FLAG_VERTEX_INCREASE, vertex);
    runner.getPeer().send(runner.getHostName(vertexID),
        new GraphJobMessage(msg));

    alterVertexCounter(1);
  }

  @Override
  public void remove() throws IOException {
    MapWritable msg = new MapWritable();
    msg.put(GraphJobRunner.FLAG_VERTEX_DECREASE, this.vertexID);

    // Get master task peer.
    String destPeer = GraphJobRunner.getMasterTask(this.getPeer());
    runner.getPeer().send(destPeer, new GraphJobMessage(msg));

    alterVertexCounter(-1);
  }

  @Override
  public long getSuperstepCount() {
    return runner.getNumberIterations();
  }

  public void setEdges(List<Edge<V, E>> list) {
    this.edges = list;
  }

  public void addEdge(Edge<V, E> edge) {
    if (edges == null) {
      this.edges = new ArrayList<Edge<V, E>>();
    }
    this.edges.add(edge);
  }

  @Override
  public List<Edge<V, E>> getEdges() {
    return (edges == null) ? new ArrayList<Edge<V, E>>() : edges;
  }

  @Override
  public M getValue() {
    return this.value;
  }

  @Override
  public void setValue(M value) {
    this.oldValue = this.value;
    this.value = value;
  }

  public void setVertexID(V vertexID) {
    this.vertexID = vertexID;
  }

  public int getMaxIteration() {
    return runner.getMaxIteration();
  }

  public int getNumPeers() {
    return runner.getPeer().getNumPeers();
  }

  /**
   * Gives access to the BSP primitives and additional features by a peer.
   */
  public BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> getPeer() {
    return runner.getPeer();
  }

  @Override
  public long getTotalNumVertices() {
    return runner.getNumberVertices();
  }

  @Override
  public void voteToHalt() {
    this.votedToHalt = true;
  }

  void setActive() {
    this.votedToHalt = false;
  }

  public boolean isHalted() {
    return votedToHalt;
  }

  void setComputed() {
    this.lastComputedSuperstep = this.getSuperstepCount();
  }

  public boolean isComputed() {
    return (lastComputedSuperstep == this.getSuperstepCount()) ? true : false;
  }

  void setVotedToHalt(boolean votedToHalt) {
    this.votedToHalt = votedToHalt;
  }

  @Override
  public int hashCode() {
    return ((vertexID == null) ? 0 : vertexID.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Vertex<?, ?, ?> other = (Vertex<?, ?, ?>) obj;
    if (vertexID == null) {
      if (other.vertexID != null)
        return false;
    } else if (!vertexID.equals(other.vertexID))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Active: " + !votedToHalt + " -> ID: " + getVertexID()
        + (getValue() != null ? " = " + getValue() : "") + " // " + edges;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      if (this.vertexID == null) {
        this.vertexID = GraphJobRunner.createVertexIDObject();
      }
      this.vertexID.readFields(in);
    }
    if (in.readBoolean()) {
      if (this.value == null) {
        this.value = GraphJobRunner.createVertexValue();
      }
      this.value.readFields(in);
    }

    this.lastComputedSuperstep = in.readLong();

    this.edges = new ArrayList<Edge<V, E>>();
    if (in.readBoolean()) {
      int num = in.readInt();
      if (num > 0) {
        for (int i = 0; i < num; ++i) {
          V vertex = GraphJobRunner.createVertexIDObject();
          vertex.readFields(in);
          E edgeCost = null;
          if (in.readBoolean()) {
            edgeCost = GraphJobRunner.createEdgeCostObject();
            edgeCost.readFields(in);
          }
          Edge<V, E> edge = new Edge<V, E>(vertex, edgeCost);
          this.edges.add(edge);
        }

      }
    }
    votedToHalt = in.readBoolean();

    boolean hasMoreContents = in.readBoolean();
    if (hasMoreContents) {
      readState(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (vertexID == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      vertexID.write(out);
    }

    if (value == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      value.write(out);
    }

    out.writeLong(lastComputedSuperstep);

    if (this.edges == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      out.writeInt(this.edges.size());
      for (Edge<V, E> edge : this.edges) {
        edge.getDestinationVertexID().write(out);
        if (edge.getValue() == null) {
          out.writeBoolean(false);
        } else {
          out.writeBoolean(true);
          edge.getValue().write(out);
        }
      }
    }
    out.writeBoolean(votedToHalt);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput customOut = new DataOutputStream(baos);
    boolean hasMoreContents = true;
    try {
      writeState(customOut);
    } catch (NullPointerException e) {
      // do nothing
    }

    // if all states are null, set hasContents to false.
    if (baos.size() == 0) {
      hasMoreContents = false;
    }

    out.writeBoolean(hasMoreContents);
    if (hasMoreContents)
      out.write(baos.toByteArray());
  }

  // compare across the vertex ID
  @SuppressWarnings("unchecked")
  @Override
  public final int compareTo(VertexInterface<V, E, M> o) {
    return getVertexID().compareTo(o.getVertexID());
  }

  /**
   * Read the state of the vertex from the input stream. The framework would
   * have already constructed and loaded the vertex-id, edges and voteToHalt
   * state. This function is essential if there is any more properties of vertex
   * to be read from.
   */
  public void readState(DataInput in) throws IOException {

  }

  /**
   * Writes the state of vertex to the output stream. The framework writes the
   * vertex and edge information to the output stream. This function could be
   * used to save the state variable of the vertex added in the implementation
   * of object.
   */
  public void writeState(DataOutput out) throws IOException {

  }

  protected void setRunner(GraphJobRunner<V, E, M> runner) {
    this.runner = runner;
  }

  protected GraphJobRunner<V, E, M> getRunner() {
    return runner;
  }

  @Override
  public void aggregate(int index, M value) throws IOException {
    this.runner.getAggregationRunner().aggregateVertex(index, oldValue, value);
  }

  /**
   * Get the last aggregated value of the defined aggregator, null if nothing
   * was configured or not returned a result. You have to supply an index, the
   * index is defined by the order you set the aggregator classes in
   * {@link GraphJob#setAggregatorClass(Class...)}. Index is starting at zero,
   * so if you have a single aggregator you can retrieve it via
   * {@link GraphJobRunner#getLastAggregatedValue}(0).
   */
  @SuppressWarnings("unchecked")
  @Override
  public M getAggregatedValue(int index) {
    return (M) runner.getLastAggregatedValue(index);
  }

  /**
   * Get the number of aggregated vertices in the last superstep. Or null if no
   * aggregator is available.You have to supply an index, the index is defined
   * by the order you set the aggregator classes in
   * {@link GraphJob#setAggregatorClass(Class...)}. Index is starting at zero,
   * so if you have a single aggregator you can retrieve it via
   * {@link #getNumLastAggregatedVertices}(0).
   */
  public IntWritable getNumLastAggregatedVertices(int index) {
    return runner.getNumLastAggregatedVertices(index);
  }

  @Override
  public Counter getCounter(Enum<?> name) {
    return runner.getPeer().getCounter(name);
  }

  @Override
  public Counter getCounter(String group, String name) {
    return runner.getPeer().getCounter(group, name);
  }
}
