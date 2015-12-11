/**
 *	@file InitializationDriver.java
 *	@brief Driver of the Job responsible for transforming the adjacent list into a list of pair <nodeID, neighborID>.
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

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**	Driver of the Job responsible for transforming the adjacent list into a list of pair <nodeID, neighborID>. */
public class InitializationDriver extends Configured implements Tool
{	
	/** Counter used to count the number of nodes in the graph. */
	public enum UtilCounters { NUM_NODES };
	
	private final String graphInput;
	private final String graphOutput;
	private final boolean verbose;
	private long numNodes;
	
	/**
	* Initializes a new instance of the InitializationDriver class.
	* @param graphInput	path of the input graph stored on hdfs.
	* @param verbose	if <c>true</c> shows on screen the messages of the Job execution.
	*/
	public InitializationDriver( String graphInput, boolean verbose )
	{
		this.graphInput = graphInput;
		// Writes the output onto the directory which will be taken in input during the first loop iteration
		this.graphOutput = FilenameUtils.removeExtension( graphInput ) + "_0";
		this.verbose = verbose;
	}
	
	/**
	 * Execute the InitializationDriver Job.
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
		
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( Text.class );
	
		job.setMapperClass( InitializationMapper.class );
		job.setNumReduceTasks( 0 );
	
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( TextOutputFormat.class );
	
		FileInputFormat.addInputPath( job, new Path( this.graphInput ) );
		FileOutputFormat.setOutputPath( job, new Path( this.graphOutput ) );
		
		if ( !job.waitForCompletion( verbose ) )
			return 1;
		
		this.numNodes = job.getCounters().findCounter( UtilCounters.NUM_NODES ).getValue();
		return 0;
	}
	
	/**
	 * Return the number of nodes in the graph.
	 * @return 	number of nodes.
	 */
	public long getNumNodes()
	{
		return this.numNodes;
	}
}