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
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Finding max vertex value in a graph.
 */
public class FindMaxGraphElement {

  public static final Log LOG = LogFactory.getLog(FindMaxGraphElement.class);

	public static class FindMaxVertex extends
      Vertex<Text, IntWritable, IntWritable> {

      	
	    @Override
	    public void setup(HamaConfiguration conf) {
	      IntWritable val = new IntWritable(Integer.parseInt(this.getVertexID().toString()));
	      this.setValue(val);
	    }

	    @Override
	    public void compute(Iterable<IntWritable> messages) throws IOException {
	    	
	    	boolean changed=false;
        int vtxId = Integer.parseInt(this.getVertexID().toString());
	    	for(IntWritable msg : messages)
        	{
              if (msg != null){
              //LOG.info("Vertex "+ vtxId +" received msg="+msg.get());
            	if(this.getValue().get() < msg.get())
            	{
                	this.setValue(msg);
                	changed=true;
            	}
            }
        	}
        
        	if (this.getSuperstepCount() == 0 || changed){
        		sendMessageToNeighbors(this.getValue());
        	}

	    	voteToHalt();

        }

      
    }

    public static class FindMaxTextReader extends
      VertexInputReader<LongWritable, Text, Text, IntWritable, IntWritable> {

    	@Override
      public boolean parseVertex(LongWritable key, Text value,
          Vertex<Text, IntWritable, IntWritable> vertex) throws Exception {

          String[] tokenArray = value.toString().split("\t");
          String vtx = tokenArray[0].trim();
          //String checkVtx = tokenArray[0].trim().split(" ");
          if (vtx.contains(" ")){
            LOG.info("Invalid Vertex:"+vtx);
          }
          if (vtx.equals("")){
            return false;
          } 

          vertex.setVertexID(new Text(vtx));
          

          //LOG.info("Parsing vertex="+vtx);
          if (tokenArray.length >= 2){
            String[] edges = tokenArray[1].trim().split(" ");
            //int size = edge.length;
            
            for (String v : edges) {
              int edge = Integer.parseInt(v);
              if (edge>=0 && edge <= 2994207){
                vertex.addEdge(new Edge<Text, IntWritable>(new Text(v), null));
              } 
              
            }
          } 

          return true;
      }
    }  



    public static GraphJob createJob(String[] args, HamaConfiguration conf,
      Options opts) throws IOException, ParseException {
    	CommandLine cliParser = new GnuParser().parse(opts, args);

    	if (!cliParser.hasOption("i") || !cliParser.hasOption("o")) {
      		System.out.println("No input or output path specified for FindMax, exiting.");
    	}

    	GraphJob maxJob = new GraphJob(conf, FindMaxGraphElement.class);
    	maxJob.setJobName("Find Max Element");

	    maxJob.setVertexClass(FindMaxVertex.class);
	    maxJob.setInputPath(new Path(cliParser.getOptionValue("i")));
	    maxJob.setOutputPath(new Path(cliParser.getOptionValue("o")));

	    // set the defaults
	    maxJob.setMaxIteration(Integer.MAX_VALUE);
	    if (cliParser.hasOption("t")) {
      		maxJob.setNumBspTask(Integer.parseInt(cliParser.getOptionValue("t")));
    	}
      maxJob.set("hama.graph.self.ref", "true");

    	// Vertex reader
    	// According to file type, which is Text
    	// Vertex reader handle it differently.
    	if (cliParser.hasOption("f")) {
      		if (cliParser.getOptionValue("f").equals("text")) {
        		maxJob.setVertexInputReaderClass(FindMaxTextReader.class);
      		} 
      		else {
        		System.out.println("File type is not available to run Find Max... "
            		+ "File type set default value, Text.");
        		maxJob.setVertexInputReaderClass(FindMaxTextReader.class);
      		}
    	} else {
      		maxJob.setVertexInputReaderClass(FindMaxTextReader.class);
    	}


    	maxJob.setVertexIDClass(Text.class);
    	maxJob.setVertexValueClass(IntWritable.class);
    	maxJob.setEdgeValueClass(NullWritable.class);

	    maxJob.setInputFormat(TextInputFormat.class);
	    maxJob.setInputKeyClass(LongWritable.class);
	    maxJob.setInputValueClass(Text.class);

	    maxJob.setPartitioner(HashPartitioner.class);
	    maxJob.setOutputFormat(TextOutputFormat.class);
	    maxJob.setOutputKeyClass(Text.class);
	    maxJob.setOutputValueClass(IntWritable.class);
	    return maxJob;
    }


    public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException, ParseException {
	    Options opts = new Options();
    	opts.addOption("i", "input_path", true, "The Location of output path.");
    	opts.addOption("o", "output_path", true, "The Location of input path.");
    	opts.addOption("h", "help", false, "Print usage");
    	opts.addOption("t", "task_num", true, "The number of tasks.");
    	opts.addOption("f", "file_type", true, "The file type of input data. Input"
        	+ "file format which is \"text\" tab delimiter separated or \"json\"."
        	+ "Default value - Text");

    	if (args.length < 2) {
      		new HelpFormatter().printHelp("findmax -i INPUT_PATH -o OUTPUT_PATH "
          		+ "[-t NUM_TASKS] [-f FILE_TYPE]", opts);
     		System.exit(-1);
    	}
    	HamaConfiguration conf = new HamaConfiguration();
    	GraphJob maxJob = createJob(args, conf, opts);
    	long startTime = System.currentTimeMillis();
    	if (maxJob.waitForCompletion(true)) {
      		System.out.println("Job Finished in "
          		+ (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    	}
    }

}