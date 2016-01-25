/**
 *	@file TranslatorDriver.java
 *	@brief Driver of the Job responsible for translating the SequenceFileOutputFormat into TextOutputFormat and vice versa.
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * Driver of the Job responsible for translating
 * the SequenceFileOutputFormat into TextOutputFormat or vice versa.
 * This is useful in order to execute the test procedures onto the output files.
 */
public class TranslatorDriver extends Configured implements Tool
{	
	/**
	 * The TranslatorDriver can translate:
	 * - \see pad.NodesPairWritable into Text
	 * - Text into \see pad.NodesPairWritable
	 * - \see pad.ClusterWritable into Text
	 * - Text into \see pad.ClusterWritable
	 */
	public enum TranslationType { Pair2Text, Text2Pair, Cluster2Text, Text2Cluster};
	
	private final Path input, output;
	private final TranslationType type;
	
	/**
	* Initializes a new instance of the TranslatorDriver class.
	* @param input		path of the input stored on hdfs.
	* @param type	which translation to perform, \ref TranslationType.
	*/
	public TranslatorDriver( TranslationType type, Path input, Path output )
	{
		this.input = input;
		this.output = output;
		this.type = type;
	}
	
	/**
	 * Execute the TranslatorDriver Job.
	 * @param args		array of external arguments, not used in this method
	 * @return 			<c>1</c> if the TranslatorDriver Job failed its execution; <c>0</c> if everything is ok. 
	 * @throws Exception 
	 */
	public int run( String[] args ) throws Exception
	{
		Configuration conf = new Configuration();
		// GenericOptionsParser invocation in order to suppress the hadoop warning.
		new GenericOptionsParser( conf, args );
		Job job = new Job( conf, "TranslatorDriver " + this.type.toString() );
		job.setJarByClass( TranslatorDriver.class );
		
		job.setNumReduceTasks( 0 );
		
		switch ( this.type )
		{
			case Pair2Text: case Cluster2Text:
				job.setMapperClass( Mapper.class );
				job.setOutputKeyClass( Text.class );
				job.setOutputValueClass( Text.class );
				job.setInputFormatClass( SequenceFileInputFormat.class );
				job.setOutputFormatClass( TextOutputFormat.class );
				break;
			case Text2Pair:
				job.setMapperClass( TranslatorMapperT2P.class );
				job.setOutputKeyClass( IntWritable.class );
				job.setOutputValueClass( IntWritable.class );
				job.setInputFormatClass( TextInputFormat.class );
				job.setOutputFormatClass( SequenceFileOutputFormat.class );		
				break;
			case Text2Cluster:
				job.setMapperClass( TranslatorMapperT2C.class );
				job.setOutputKeyClass( ClusterWritable.class );
				job.setOutputValueClass( NullWritable.class );
				job.setInputFormatClass( TextInputFormat.class );
				job.setOutputFormatClass( SequenceFileOutputFormat.class );	
				break;
		}
	
		FileInputFormat.addInputPath( job, this.input );
		FileOutputFormat.setOutputPath( job, this.output );
		
		if ( !job.waitForCompletion( true ) )
			return 1;
	
		return 0;
	}
	
	/**
	 * Main of the \see TranslatorDriver class.
	 * @param args	array of external arguments,
	 * @throws Exception
	 */
	public static void main( String[] args ) throws Exception 
	{	
		if ( args.length != 3 )
		{
			System.out.println( "Usage: TranslatorTest <type> <input> <output>" );
			System.exit(1);
		}
		
		TranslationType type;
		if ( args[0].toLowerCase().equals( "pair2text" ) )
			type = TranslationType.Pair2Text;
		else if (args[0].toLowerCase().equals( "text2pair" ) )
			type = TranslationType.Text2Pair;
		else if (args[0].toLowerCase().equals( "cluster2text" ) )
			type = TranslationType.Cluster2Text;
		else
			type = TranslationType.Text2Cluster;
		
		Path input = new Path( args[1] );
		Path output = new Path( args[2] );
		System.out.println( "Start TranslatorDriver " + type.toString() + "." );
		TranslatorDriver trans = new TranslatorDriver( type, input, output );
		if ( trans.run( null ) != 0  )
		{
			FileSystem.get( new Configuration() ).delete( output, true  );
			System.exit( 1 );
		}
		System.out.println( "End TranslatorDriver " + type.toString() + "." );

		System.exit( 0 );
	}
}