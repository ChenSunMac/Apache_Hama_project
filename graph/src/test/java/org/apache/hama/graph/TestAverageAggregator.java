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

import junit.framework.TestCase;

import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;

public class TestAverageAggregator extends TestCase {

  @Test
  public void testAggregator() {
    AverageAggregator diff = new AverageAggregator();
    diff.aggregate(new DoubleWritable(5), new DoubleWritable(2));
    diff.aggregateInternal();
    diff.aggregate(new DoubleWritable(5), new DoubleWritable(2));
    diff.aggregateInternal();
    diff.aggregate(null, new DoubleWritable(5));
    diff.aggregateInternal();

    assertEquals(3, diff.getTimesAggregated().get());
    DoubleWritable x = diff.finalizeAggregation();
    assertEquals(2, (int) x.get());

  }

}
