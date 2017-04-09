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


import java.io.*;
import java.util.*;

import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.graph.GraphJobMessage;
import org.apache.hama.graph.AverageAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;
import org.apache.hama.graph.VertexOutputWriter;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Synchronous label propagation aglorithm.
 */
public class LabelPropagation {

	public static final Log LOG = LogFactory.getLog(LabelPropagation.class);

  public static class Label implements Writable {
      private int current;
      private int last;
      private int source;
      private int numEdges;
      private int osCount;
      private Map<Integer, Integer> lbCounters;

      Label (){
        this.source = -1;
        this.current = -1;
        this.last = -1;
        this.numEdges = 0;
        this.osCount = 0;
        this.lbCounters = new HashMap<Integer, Integer>();
      }

      Label (int current){
        this.source = current;
        this.current = current;
        this.last = current;
        this.numEdges = 0;
        this.osCount = 0;
        this.lbCounters = new HashMap<Integer, Integer>();
      }

      Label (int source, int current, int last){
        this.source = source;
        this.current = current;
        this.last = last;
        this.lbCounters = null;
      }


      public Map<Integer, Integer> getLabelCounters(){
        return this.lbCounters;
      }

      public void clearLabelCounters(){
        this.lbCounters.clear();
      }

      public Integer getCurrent(){
        return this.current;
      }

      public Integer getLast(){
        return this.last;
      }

      public Integer getSource(){
        return this.source;
      }

      public Integer getNumEdges(){
        return this.numEdges;
      }

      public Integer getOscillationCount(){
        return this.osCount;
      }
      public void setCurrent(int current){
        this.current = current;
      } 

      public void setLast(int last){
        this.last = last;
      }

      public void setNumEdges(int numEdges){
        this.numEdges = numEdges;
      }

      public void setOscillationCount(int osCount){
        this.osCount = osCount;
      }
  
      @Override
       public void readFields(DataInput in) throws IOException {
              current = in.readInt();
              last = in.readInt();
              source = in.readInt();
              numEdges = in.readInt();
              osCount = in.readInt();
       }
       @Override
       public void write(DataOutput out) throws IOException {
              out.writeInt(current);
              out.writeInt(last);
              out.writeInt(source);
              out.writeInt(numEdges);
              out.writeInt(osCount);
       }

  }

	public static class LabelPropVertex extends
      Vertex<Text, Label, Label> {

	    // @Override
	    // public void setup(HamaConfiguration conf) {
	    //   Label val = new Label(Integer.parseInt(this.getVertexID().toString()));
     //    int numEdges = 0;
     //    for (Edge<Text, Label> e : this.getEdges()) {
     //      numEdges++;
     //    }
     //    val.setNumEdges(numEdges);
     //    val.setOscillationCount(0);
	    //   this.setValue(val);
	    // }

	    @Override
	    public void compute(Iterable<Label> messages) throws IOException {
	    	
	    	if (this.getSuperstepCount() == 0){
          //LOG.info("Superstep: "+ this.getSuperstepCount());
	    		Label val = new Label(Integer.parseInt(this.getVertexID().toString()));
          //LOG.info("my source is: "+val.getSource());
          int numEdges = 0;
          for (Edge<Text, Label> e : this.getEdges()) {
            numEdges++;
          }
          val.setNumEdges(numEdges);
          val.setOscillationCount(0);
	    		this.setValue(val);
	    		sendMessageToNeighbors(this.getValue());
	    	}
	    	else {
          //LOG.info("Superstep: "+ this.getSuperstepCount());
	    		int msglabel, clabel, plabel,slabel, msgSource;
	    		int mflabel;
	    		//Map<Integer, Integer> lbCounters = new HashMap<Integer, Integer>();
          Map<Integer, Integer> lbCounters = this.getValue().getLabelCounters();
          clabel = this.getValue().getCurrent();
          plabel = this.getValue().getLast();
          
          //LOG.info("PARSING MESSAGES OF "+this.getValue().getSource());
          // PARSING MESSAGES AND STORING THEM IN EDGES
	    		for(Label msg : messages){
            if (msg != null){
            msglabel = msg.getCurrent();
            msgSource = msg.getSource();
            //LOG.info("msgSource="+msg.getSource()+" msgCurrent="+msglabel+" msgLast="+msg.getLast());

            for (Edge<Text, Label> e : this.getEdges()) {
              int destination = Integer.parseInt(e.getDestinationVertexID().toString());
              //LOG.info("destination="+destination);
              if (msgSource == destination){
                //LOG.info("WE ARE SETTING EDGE VALUE");
                e.setValue(msg);
              }
            }
          }
          }

          //LOG.info("PARSING EDGES OF "+this.getValue().getSource());
          // PARSING EDGES AND PLACING LABELS IN HASHTABLE OF COUNTERS
          for (Edge<Text, Label> e : this.getEdges()) {
            
            if (e.getValue()!=null){
              msglabel = e.getValue().getCurrent();
              //LOG.info("Vertex " +this.getVertexID() + " received message: "+msglabel);
  	    			if (lbCounters.get(msglabel) != null){
  	    				int count = lbCounters.get(msglabel);
  	    				count++;
  	    				lbCounters.put(msglabel, count);
  	    			} else {
  	    				lbCounters.put(msglabel, 0);
  	    			}
            }
	    		}

         
	    		mflabel = findMostFrequentLabel(lbCounters);
          if (mflabel == -1){
            lbCounters.put(mflabel, -1);
          }
          lbCounters.put(clabel, -1);
	    		//LOG.info("Most Frequent Label: " + mflabel+
	    		//	" of Vertex: "+this.getVertexID() +" clabel: "+clabel + " plabel: "+plabel);


          if (plabel==mflabel){
            int osCount = this.getValue().getOscillationCount();
            osCount++;
            this.getValue().setOscillationCount(osCount);
          }
          boolean stable;
          if (this.getValue()!=null && this.getValue().getOscillationCount()>=10){
            stable = false;
          } else {
            stable = true;
          }

	    		if (lbCounters.get(mflabel) > lbCounters.get(clabel) && stable){ 
	    			this.getValue().setCurrent(mflabel);
            this.getValue().setLast(clabel);
            this.getValue().clearLabelCounters();
            sendMessageToNeighbors(this.getValue());
	    		} 


	    	}

	    	voteToHalt();

        }

        public int findMostFrequentLabel(Map<Integer, Integer> lbCounters){
        
			    
        	int mostFrequentLabel = -1;
        	int maxFreq = -1;
          List<Integer> labels = new ArrayList<Integer>(lbCounters.keySet());
          List<Integer> filteredLabels = new ArrayList<Integer>();

          if (labels.size() == 0){
            return mostFrequentLabel;
          }
          
        	for (Integer label: labels) {
            int freq = (int)lbCounters.get(label);
        		if( freq > maxFreq) { 
        			mostFrequentLabel = (int)label;
        			maxFreq = freq;
        		}	
    		  }

          for (Integer label: labels) {
            int freq = (int)lbCounters.get(label);
            if (lbCounters.get(label)!=null && maxFreq == freq){
             filteredLabels.add(label);
            }
          }
          //LOG.info("vtx="+ this.getVertexID()+" maxFreq="+maxFreq);

          Random randomGenerator = new Random();

          if (filteredLabels.size()>1){
               int randomIndex = randomGenerator.nextInt(filteredLabels.size());
               mostFrequentLabel = filteredLabels.get(randomIndex);
          }

    		  return mostFrequentLabel;
        }


      
    }




	public static class LabelPropTextReader extends
      VertexInputReader<LongWritable, Text, Text, Label, Label> {

    	@Override
    	public boolean parseVertex(LongWritable key, Text value,
        	Vertex<Text, Label, Label> vertex) throws Exception {

      		String[] tokenArray = value.toString().split("\t");
      		String vtx = tokenArray[0].trim();
          if (vtx.equals("")){
            return false;
          } 

          vertex.setVertexID(new Text(vtx));
          

          //LOG.info("Parsing vertex="+vtx);
      		if (tokenArray.length >= 2){
            String[] edges = tokenArray[1].trim().split(" ");
            int count = 0;
            for (String v : edges) {
              int edge = Integer.parseInt(v);
              if (edge>=0 && edge <= 6718392){
                vertex.addEdge(new Edge<Text, Label>(new Text(v), null));
              } 
             
            }
          } 

      		return true;
    	}
    } 

  public static class LabelPropTextWriter implements 
    VertexOutputWriter<Text, Text, Text,  Label, Label>{

      @Override
      public void setup(Configuration conf) {
        // do nothing
      }

      @SuppressWarnings("unchecked")
      @Override
      public void write(Vertex<Text, Label, Label> vertex,
          BSPPeer<Writable, Writable, Text, Text, GraphJobMessage> peer)
          throws IOException {
        Label vertexValue = (Label) vertex.getValue();
        if (vertexValue !=null){
          int clabel = vertexValue.getCurrent();
          peer.write((Text) vertex.getVertexID(), (Text) new Text(Integer.toString(clabel)));
        }
      }
    }


	public static GraphJob createJob(String[] args, HamaConfiguration conf,
      Options opts) throws IOException, ParseException {
    	CommandLine cliParser = new GnuParser().parse(opts, args);

    	if (!cliParser.hasOption("i") || !cliParser.hasOption("o")) {
      		System.out.println("No input or output path specified for LabelProp, exiting.");
    	}

    	GraphJob labelJob = new GraphJob(conf, LabelPropagation.class);
    	labelJob.setJobName("Label Propagation");

	    labelJob.setVertexClass(LabelPropVertex.class);
	    labelJob.setInputPath(new Path(cliParser.getOptionValue("i")));
	    labelJob.setOutputPath(new Path(cliParser.getOptionValue("o")));

	    // set the defaults
	    labelJob.setMaxIteration(Integer.MAX_VALUE);
	    if (cliParser.hasOption("t")) {
      		labelJob.setNumBspTask(Integer.parseInt(cliParser.getOptionValue("t")));
    	}


    	// Vertex reader
    	// According to file type, which is Text
    	// Vertex reader handle it differently.
    	if (cliParser.hasOption("f")) {
      		if (cliParser.getOptionValue("f").equals("text")) {
        		labelJob.setVertexInputReaderClass(LabelPropTextReader.class);
      		} 
      		else {
        		System.out.println("File type is not available to run Find Max... "
            		+ "File type set default value, Text.");
        		labelJob.setVertexInputReaderClass(LabelPropTextReader.class);
      		}
    	} else {
      		labelJob.setVertexInputReaderClass(LabelPropTextReader.class);
    	}


    	labelJob.setVertexIDClass(Text.class);
    	labelJob.setVertexValueClass(Label.class);
    	labelJob.setEdgeValueClass(Label.class);

	    labelJob.setInputFormat(TextInputFormat.class);
	    labelJob.setInputKeyClass(LongWritable.class);
	    labelJob.setInputValueClass(Text.class);

	    labelJob.setPartitioner(HashPartitioner.class);
      labelJob.setVertexOutputWriterClass(LabelPropTextWriter.class);
	    labelJob.setOutputFormat(TextOutputFormat.class);
	    labelJob.setOutputKeyClass(Text.class);
	    labelJob.setOutputValueClass(Text.class);
	    return labelJob;
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
      		new HelpFormatter().printHelp("labelprop -i INPUT_PATH -o OUTPUT_PATH "
          		+ "[-t NUM_TASKS] [-f FILE_TYPE]", opts);
     		System.exit(-1);
    	}
    	HamaConfiguration conf = new HamaConfiguration();
    	GraphJob labelJob = createJob(args, conf, opts);
    	long startTime = System.currentTimeMillis();
    	if (labelJob.waitForCompletion(true)) {
      		System.out.println("Job Finished in "
          		+ (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    	}

    }
}