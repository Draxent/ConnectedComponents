/**
 *	@file InitializationDriver.java
 *	@brief Driver of the Job responsible for transforming the adjacency list or cluster list into a list of pair <nodeID, neighborID>.
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**	Driver of the Job responsible for transforming the adjacency list or cluster list into a list of pair <nodeID, neighborID>. */
public class InitializationDriver extends Configured implements Tool
{	
	/** The input file can be format as an adjacency list or a cluster list */
	public enum InputType { ADJACENCY_LIST, CLUSTER_LIST };
	/** Directory name for multiple output */
	public static final String MOS_OUTPUT_NAME = "result";
	/** Base output path for multiple output */
	public static final String MOS_BASEOUTPUTPATH = MOS_OUTPUT_NAME + "/part";
	
	private final Path input, output;
	private final boolean verbose;
	private InputType type;
	private long numInitialClusters, numInitialNodes;
	
	/**
	* Initializes a new instance of the InitializationDriver class.
	* @param input		path of the input graph stored on hdfs.
	* @param output		path of the output folder.
	* @param verbose	if <c>true</c> shows on screen the messages of the Job execution.
	* @throws IOException 
	*/
	public InitializationDriver( Path input, Path output, boolean verbose ) throws IOException
	{
		this.input = input;
		this.output = output;
		this.verbose = verbose;
		
		// Analyze the first line of the input file in order to determine
		// if is format as an adjacency list or a cluster list.
        FileSystem fs = FileSystem.get( new Configuration() );
        BufferedReader br = new BufferedReader( new InputStreamReader( fs.open( this.input ) ) );
        
        // Repeat until we succeed to classify the input file.
        boolean done = false;
        while ( !done )
        {
	        // Read line
	        String line = br.readLine();
	        // Split the line on the tab character.
	        String userID_neighborhood[] = line.split( "\t" );
	        // If <TAB> not found, the format of input file can be cluster format or the node is alone.
	        if( userID_neighborhood.length == 1 )
	        {
				// Split the line on the space character.
				String clusterLists[] = line.split( " " );
				
				// If the node is alone we have to repeat the procedure,
				// since we cannot understand the format analyzing this line.
				if ( clusterLists.length > 1 )
				{
					this.type = InputType.CLUSTER_LIST;
					done = true;
				}
	        }
	        else
	        {
	        	this.type = InputType.ADJACENCY_LIST;
	        	done = true;
	        }
        }
        
        // Close file
        br.close();
	}
	
	/**
	 * Execute the InitializationDriver Job.
	 * 
	 * If the input file format is adjacency list, then we can easily determinate the initial number of nodes
	 * that is equal to the number of rows of the input file while the initial number of clusters is zero.
	 * In order to obtain a list of arcs from the adjacency list, we use the \see InitializationMapperAdjacent
	 * as Mapper and zero Reducer.
	 * 
	 * If the input file format is cluster list, then we can easily determinate the initial number of clusters
	 * that is equal to the number of rows of the input file.
	 * In order to obtain a list of arcs from the cluster list, we use the \see InitializationMapperCluster
	 * as Mapper. We store this result into a special folder \see MOS_OUTPUT_NAME.
	 * Into the regular folder, this Mapper emits all the encountered nodes.
	 * We use \see InitializationReducerNumNodes as Reducer in order to count the initial number of nodes
	 * counting all the distinct nodes found. The combiner (\see InitializationCombinerNumNodes) reduce locally
	 * the number of duplicated nodes.
	 * Obtained the value of the NUM_INITIAL_NODES counter ( \see UtilCounters ), we delete the empty files
	 * produced by the Reducer and we move the real results into the main/regular folder.
	 * 
	 * @param args		array of external arguments, not used in this method
	 * @return 			<c>1</c> if the InitializationDriver Job failed its execution; <c>0</c> if everything is ok. 
	 * @throws Exception 
	 */
	public int run( String[] args ) throws Exception
	{
		Configuration conf = new Configuration();
		// GenericOptionsParser invocation in order to suppress the hadoop warning.
		new GenericOptionsParser( conf, args );
		Job job = new Job( conf, "InitializationDriver" );
		job.setJarByClass( InitializationDriver.class );
		
		job.setOutputKeyClass( IntWritable.class );
		job.setOutputValueClass( IntWritable.class );
		
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( SequenceFileOutputFormat.class );
		
		FileInputFormat.addInputPath( job, this.input );
		FileOutputFormat.setOutputPath( job, this.output );
		
		if ( this.type == InputType.ADJACENCY_LIST )
		{
			// In order to obtain the arcs list from the adjacency list, we need only a Mapper task.
			job.setMapperClass( InitializationMapperAdjacency.class );
			job.setNumReduceTasks( 0 );			
		}
		else
		{
			// Set up the special folder.
			MultipleOutputs.addNamedOutput( job, MOS_OUTPUT_NAME, SequenceFileOutputFormat.class, IntWritable.class, IntWritable.class );
			MultipleOutputs.setCountersEnabled( job, true );
			// In order to obtain the arcs list from the cluster list, we need only a Mapper task
			// and we save the result into the special folder.
			// Then, we need a Reducer task in order to count the initial number of nodes
			job.setMapperClass( InitializationMapperCluster.class );
			job.setCombinerClass( InitializationCombinerNumNodes.class );
			job.setReducerClass( InitializationReducerNumNodes.class  );
		}		
		
		if ( !job.waitForCompletion( verbose ) )
			return 1;
		
		// Set up the private variables looking to the counters value
		this.numInitialClusters = job.getCounters().findCounter( UtilCounters.NUM_INITIAL_CLUSTERS ).getValue();
		this.numInitialNodes = job.getCounters().findCounter( UtilCounters.NUM_INITIAL_NODES ).getValue();
		
		if ( this.type == InputType.CLUSTER_LIST )
		{
			FileSystem fs = FileSystem.get( conf );
			
			// Delete the empty outputs of the Job
			FileStatus[] filesStatus = fs.listStatus( this.output );
			for ( FileStatus fileStatus : filesStatus )
				if ( fileStatus.getPath().getName().contains( "part" ) )
					fs.delete( fileStatus.getPath(), false );
			
			// Move the real outputs into the parent folder
			filesStatus = fs.listStatus( this.output.suffix( "/" + MOS_OUTPUT_NAME ) );
			for ( FileStatus fileStatus : filesStatus )
				fs.rename( fileStatus.getPath(), this.output.suffix( "/" + fileStatus.getPath().getName() ) );
			
			// Delete empty special folder
			fs.delete( this.output.suffix( "/" + MOS_OUTPUT_NAME), true );
		}
		
		return 0;
	}
	
	/**
	 * Return the type of format of the input file.
	 * @return 	the type of format of the input file.
	 */
	public InputType getInputType()
	{
		return this.type;
	}
	
	/**
	 * Returns the number of initial clusters founds in the input file.
	 * @return 	number of initial clusters.
	 */
	public long getNumInitialClusters()
	{
		return this.numInitialClusters;
	}
	
	/**
	 * Returns the number of initial nodes founds in the input file.
	 * @return 	number of initial nodes.
	 */
	public long getNumInitialNodes()
	{
		return this.numInitialNodes;
	}
	
	/**
	 * Main of the \see InitializationDriver class.
	 * @param args	array of external arguments,
	 * @throws Exception
	 */
	public static void main( String[] args ) throws Exception 
	{	
		if ( args.length != 2 )
		{
			System.out.println( "Usage: InitializationDriver <input> <output>" );
			System.exit(1);
		}

		Path input = new Path( args[0] );
		Path output = new Path( args[1] );
		System.out.println( "Start InitializationDriver. " );
		InitializationDriver init = new InitializationDriver( input, output, true );
		if ( init.run( null ) != 0  )
		{
			FileSystem.get( new Configuration() ).delete( output, true  );
			System.exit( 1 );
		}
		System.out.println( "End InitializationDriver." );

		System.exit( 0 );
	}
}