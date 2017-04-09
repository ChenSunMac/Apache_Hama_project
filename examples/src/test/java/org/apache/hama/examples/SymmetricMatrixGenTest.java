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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.commons.io.TextArrayWritable;
import org.apache.hama.examples.util.SymmetricMatrixGen;
import org.junit.Test;

public class SymmetricMatrixGenTest {
  protected static Log LOG = LogFactory.getLog(SymmetricMatrixGenTest.class);
  private static String TEST_OUTPUT = "/tmp/test";

  @Test
  public void testGraphGenerator() throws Exception {
    Configuration conf = new Configuration();

    SymmetricMatrixGen.main(new String[] { "20", "10", TEST_OUTPUT, "3" });
    FileSystem fs = FileSystem.get(conf);

    FileStatus[] globStatus = fs.globStatus(new Path(TEST_OUTPUT + "/part-*"));
    for (FileStatus fts : globStatus) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, fts.getPath(),
          conf);
      Text key = new Text();
      TextArrayWritable value = new TextArrayWritable();

      while (reader.next(key, value)) {
        String values = "";
        for (Writable v : value.get()) {
          values += v.toString() + " ";
        }
        LOG.info(fts.getPath() + ": " + key.toString() + " | " + values);
      }
      reader.close();
    }

    fs.delete(new Path(TEST_OUTPUT), true);
  }
}
