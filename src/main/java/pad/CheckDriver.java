/**
 *	@file CheckDriver.java
 *	@brief Driver of the Job responsible for verifying if the clusters are well formed.
 *	@author Federico Conte (draxent)
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**	Driver of the Job responsible for verifying if the clusters are well formed. */
public class CheckDriver extends Configured implements Tool
{
	/** Counter used to count the number of clusters found malformed from the Reducer Tasks. */
	public enum UtilCounters { NUM_ERRORS };
	
	private final String input;
	private final boolean verbose;
	
	/**
	* Initializes a new instance of the CheckDriver class.
	* @param input		path of the result folder of \see TerminationDriver Job.
	* @param verbose	if <c>true</c> shows on screen the messages of the Job execution.
	*/
	public CheckDriver( String input, boolean verbose )
	{
		this.input = input;
		this.verbose = verbose;
	}
	
	/**
	 * Execute the CheckDriver Job and, in case of success, renames the output files properly.
	 * @param args		array of external arguments, not used in this method
	 * @return 			<c>1</c> if the CheckDriver Job failed its execution or the Cluster are malformed;
	 * 					<c>0</c> if everything is ok. 
	 * @throws Exception
	 */
	public int run( String[] args ) throws Exception
	{
		Configuration conf = new Configuration();
		// GenericOptionsParser invocation in order to suppress the hadoop warning.
		new GenericOptionsParser( conf, args );
		Job job = new Job( conf, "CheckDriver" );
		job.setJarByClass( CheckDriver.class );
		
		job.setMapOutputKeyClass( IntWritable.class );
		job.setMapOutputValueClass( NullWritable.class );
		
		job.setMapperClass( CheckMapper.class );
		job.setReducerClass( CheckReducer.class );
	
		Path inputPath = new Path ( this.input );
		Path outputPath = new Path( this.input + "_check" );
		job.setInputFormatClass( TextInputFormat.class );
		FileInputFormat.addInputPath( job, inputPath );
		FileOutputFormat.setOutputPath( job, outputPath );
		
		if ( !job.waitForCompletion( verbose ) )
			return 1;
		
		// Clusters are malformed
		if ( job.getCounters().findCounter( UtilCounters.NUM_ERRORS ).getValue() > 0 )
			return 1;
		
		// The Clusters seems correct, so we can arrange better the output files
		FileSystem fs = FileSystem.get( conf );
		FileStatus[] filesStatus = fs.listStatus( inputPath );
		
		int num_cluster = 0;
		for ( FileStatus fileStatus : filesStatus )
		{
			Path p = fileStatus.getPath();
			String name = p.getName();
			
			// Rename Cluster in increasing order
			if ( name.contains( "cluster_" ) )
			{
				fs.rename( p, inputPath.suffix( "/cluster_" + num_cluster ) );
				num_cluster++;
			}
			// Delete the empty output produced by the Reducers
			else if ( name.contains( "part" ) )
				fs.delete( p, false );
		}
		
		// Delete outputPath
		fs.delete( outputPath, true );
		return 0;
	}
}