/**
 *	@file CheckDriver.java
 *	@brief Driver of the Job responsible for verifying if the clusters are well formed.
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

/**	Driver of the Job responsible for verifying if the clusters are well formed. */
public class CheckDriver extends Configured implements Tool
{	
	private final Path input;
	private final boolean verbose;
	private boolean testOk;
	
	/**
	* Initializes a new instance of the CheckDriver class.
	* @param input		path of the input graph stored on hdfs.
	* @param verbose	if <c>true</c> shows on screen the messages of the Job execution.
	*/
	public CheckDriver( Path input, boolean verbose )
	{
		this.input = input;
		this.verbose = verbose;
	}
	
	/**
	 * Execute the CheckDriver Job.
	 * @param args		array of external arguments, not used in this method
	 * @return 			<c>1</c> if the CheckDriver Job failed its execution; <c>0</c> if everything is ok. 
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
		job.setOutputKeyClass( NullWritable.class );
		job.setOutputValueClass( NullWritable.class );
	
		job.setMapperClass( CheckMapper.class );
		job.setReducerClass( CheckReducer.class );
	
		job.setInputFormatClass( SequenceFileInputFormat.class );
		job.setOutputFormatClass( SequenceFileOutputFormat.class );
	
		FileInputFormat.addInputPath( job, this.input );
		FileOutputFormat.setOutputPath( job, this.input.suffix("_check") );
		
		if ( !job.waitForCompletion( verbose ) )
			return 1;
		
		// Set up the private variable looking to the counter value
		this.testOk = ( job.getCounters().findCounter( UtilCounters.NUM_ERRORS ).getValue() == 0 );
		
		// Delete the output folder ( we did not write on it )
		FileSystem.get( conf ).delete( input.suffix("_check"), true  );
		
		return 0;
	}
	
	/**
	 * Return <code>false</code> if the checking phase has found that at least one Cluster is malformed,
	 * <code>true</code> otherwise.
	 * @return 	<code>true</code> if no Cluster is malformed, <code>false</code> otherwise.
	 */
	public boolean isTestOk()
	{
		return this.testOk;
	}
	
	/**
	 * Main of the \see CheckDriver class.
	 * @param args	array of external arguments,
	 * @throws Exception
	 */
	public static void main( String[] args ) throws Exception 
	{	
		if ( args.length != 1 )
		{
			System.out.println( "Usage: CheckDriver <input>" );
			System.exit(1);
		}

		Path input = new Path( args[0] );
		System.out.println( "Start CheckDriver. " );
		CheckDriver check = new CheckDriver( input, true );
		if ( check.run( null ) != 0  )
			System.exit( 1 );
		System.out.println( "End CheckDriver." );
		
		System.out.println( "TestOK: " + String.valueOf( check.isTestOk() ) );
		System.exit( 0 );
	}
}