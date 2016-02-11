/**
 *	@file TerminationDriver.java
 *	@brief Driver of the Job responsible for transforming the list of pair <nodeID, neighborID> into sets of nodes (clusters).
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
import org.apache.hadoop.fs.FileSystem;
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
 * Driver of the Job responsible for transforming the edges list 
 * <nodeID, neighbourID> into sets of nodes (clusters).
 */
public class TerminationDriver extends Configured implements Tool
{	
	private final Path input, output;
	private final boolean verbose;
	private long numNodes, numClusters;
	
	/**
	* Initializes a new instance of the TerminationDriver class.
	* @param input		path of the result folder of \see StarDriver Job.
	* @param output		path of the output folder.
	* @param verbose	if <c>true</c> shows on screen the messages of the Job execution.
	*/
	public TerminationDriver( Path input, Path output, boolean verbose )
	{
		this.input = input;
		this.output = output;
		this.verbose = verbose;
	}
		
	/**
	 * Execute the TerminationDriver Job.
	 * @param args		array of external arguments, not used in this method
	 * @return 			<c>1</c> if the TerminationDriver Job failed its execution; <c>0</c> if everything is ok. 
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
	
		FileInputFormat.addInputPath( job, this.input );
		FileOutputFormat.setOutputPath( job, this.output );
		
		if ( !job.waitForCompletion( this.verbose ) )
			return 1;
		
		// Set up the private variables looking to the counters value
		this.numNodes = job.getCounters().findCounter( UtilCounters.NUM_NODES ).getValue();
		this.numClusters = job.getCounters().findCounter( UtilCounters.NUM_CLUSTERS ).getValue();
		return 0;
	}
	
	/**
	 * Return the number of nodes found.
	 * @return 	number of nodes.
	 */
	public long getNumNodes()
	{
		return this.numNodes;
	}
	
	/**
	 * Return the number of clusters found.
	 * @return 	number of clusters.
	 */
	public long getNumClusters()
	{
		return this.numClusters;
	}
	
	/**
	 * Main of the \see TerminationDriver class.
	 * @param args	array of external arguments,
	 * @throws Exception
	 */
	public static void main( String[] args ) throws Exception 
	{	
		if ( args.length != 2 )
		{
			System.out.println( "Usage: TerminationDriver <input> <output>" );
			System.exit(1);
		}
		
		Path input = new Path( args[0] );
		Path output = new Path( args[1] );
		System.out.println( "Start TerminationDriver. " );
		TerminationDriver term = new TerminationDriver( input, output, true );
		if ( term.run( null ) != 0  )
		{
			FileSystem.get( new Configuration() ).delete( output, true  );
			System.exit( 1 );
		}
		System.out.println( "End TerminationDriver." );

		System.exit( 0 );
	}
}
