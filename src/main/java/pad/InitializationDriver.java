/**
 *	@file InitializationDriver.java
 *	@brief Driver of the Job responsible for transforming the adjacent list or cluster list into a list of pair <nodeID, neighborID>.
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**	Driver of the Job responsible for transforming the adjacent list or cluster list into a list of pair <nodeID, neighborID>. */
public class InitializationDriver extends Configured implements Tool
{	
	private final Path graphInput;
	private final Path graphOutput;
	private final boolean verbose;
	
	/**
	* Initializes a new instance of the InitializationDriver class.
	* @param graphInput	path of the input graph stored on hdfs.
	* @param verbose	if <c>true</c> shows on screen the messages of the Job execution.
	*/
	public InitializationDriver( String graphInput, boolean verbose )
	{
		this.graphInput = new Path( graphInput );
		// Writes the output onto the directory which will be taken in input during the first loop iteration
		this.graphOutput = new Path( FilenameUtils.removeExtension( graphInput ) + "_0" );
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
		
		job.setOutputKeyClass( IntWritable.class );
		job.setOutputValueClass( IntWritable.class );
	
		job.setMapperClass( InitializationMapper.class );
		job.setNumReduceTasks( 0 );
	
		job.setInputFormatClass( TextInputFormat.class );
		job.setOutputFormatClass( SequenceFileOutputFormat.class );
	
		FileInputFormat.addInputPath( job, this.graphInput );
		FileOutputFormat.setOutputPath( job, this.graphOutput );
		
		if ( !job.waitForCompletion( verbose ) )
			return 1;
	
		return 0;
	}
}