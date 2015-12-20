/**
 *	@file TerminationDriver.java
 *	@brief Driver of the Job responsible for transforming transform the list of pair <nodeID, neighborID> into sets of nodes (clusters).
 *  @author Federico Conte (draxent)
 *  
 *	Copyright 2015 Federico Conte
 *	https://github.com/Draxent/ConnectedComponents
 * 
 *	Licensed under the Apache License, Version 2.0 (the "License"); 
 *	you may not use this file except in compliance with the License. 
 *	You may obtain a copy of the License at 
 * 
 *	http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *	Unless required by applicable law or agreed to in writing, software 
 *	distributed under the License is distributed on an "AS IS" BASIS, 
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 *	See the License for the specific language governing permissions and 
 *	limitations under the License. 
 */

package pad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**	
 * Driver of the Job responsible for transforming transform the list of pair 
 * <nodeID, neighborID> into sets of nodes (clusters).
 */
public class TerminationDriver extends Configured implements Tool
{
	/** Counter used to count the number of clusters found. */
	public enum UtilCounters { NUM_CLUSTERS };
	
	private final Path graphInput;
	private final Path output;
	private final boolean verbose;
	private int numClusters;
	
	/**
	* Initializes a new instance of the TerminationDriver class.
	* @param graphInput	path of the result folder of \see StarDriver Job.
	* @param output		path of the output folder.
	* @param verbose	if <c>true</c> shows on screen the messages of the Job execution.
	*/
	public TerminationDriver( Path graphInput, Path output, boolean verbose )
	{
		this.graphInput = graphInput;
		this.output = output;
		this.verbose = verbose;
	}
		
	/**
	 * Execute the TerminationDriver Job.
	 * @param args		array of external arguments, not used in this method
	 * @return 			<c>1</c> if the StarDriver Job failed its execution; <c>0</c> if everything is ok. 
	 * @throws Exception 
	 */
	public int run( String[] args ) throws Exception
	{
		Configuration conf = new Configuration();
		// GenericOptionsParser invocation in order to suppress the hadoop warning.
		new GenericOptionsParser( conf, args );
		Job job = new Job( conf, "TerminationDriver" );
		job.setJarByClass( TerminationDriver.class );
		
		job.setMapOutputKeyClass( NodesPairWritable.class );
		job.setMapOutputValueClass( IntWritable.class );
		job.setOutputKeyClass( ClusterWritable.class );
		job.setOutputValueClass( NullWritable.class );
		
		job.setMapperClass( TerminationMapper.class );
		job.setPartitionerClass( NodePartitioner.class );
		job.setGroupingComparatorClass( NodeGroupingComparator.class );
		job.setReducerClass( TerminationReducer.class );
	
		job.setInputFormatClass( SequenceFileInputFormat.class );
		job.setOutputFormatClass( SequenceFileOutputFormat.class );
	
		Path outputPath = this.output;
		FileInputFormat.addInputPath( job, this.graphInput );
		FileOutputFormat.setOutputPath( job, outputPath );
		
		if ( !job.waitForCompletion( this.verbose ) )
			return 1;
		
		this.numClusters = (int) job.getCounters().findCounter( UtilCounters.NUM_CLUSTERS ).getValue();
		return 0;
	}
	
	/**
	 * Return the number of clusters found.
	 * @return 	number of clusters.
	 */
	public int getNumClusters()
	{
		return this.numClusters;
	}
}