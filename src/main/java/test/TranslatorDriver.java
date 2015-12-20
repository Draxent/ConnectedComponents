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

package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
	 */
	public enum DirectionTranslation { Pair2Text, Text2Pair, Cluster2Text };
	
	private final Path input;
	private final DirectionTranslation direction;
	
	/**
	* Initializes a new instance of the TranslatorDriver class.
	* @param input		path of the input stored on hdfs.
	* @param direction	which translation to perform, \ref DirectionTranslation.
	*/
	public TranslatorDriver( Path input, DirectionTranslation direction )
	{
		this.input = input;
		this.direction = direction;
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
		Job job = new Job( conf, "TranslatorDriver " + this.direction.toString() );
		job.setJarByClass( TranslatorDriver.class );
		
		job.setNumReduceTasks( 0 );
		
		switch ( this.direction )
		{
			case Pair2Text: case Cluster2Text:
				job.setMapperClass( Mapper.class );
				job.setOutputKeyClass( Text.class );
				job.setOutputValueClass( Text.class );
				job.setInputFormatClass( SequenceFileInputFormat.class );
				job.setOutputFormatClass( TextOutputFormat.class );
				break;
			case Text2Pair:
				job.setMapperClass( Translator_Text2Pair_Mapper.class );
				job.setOutputKeyClass( IntWritable.class );
				job.setOutputValueClass( IntWritable.class );
				job.setInputFormatClass( TextInputFormat.class );
				job.setOutputFormatClass( SequenceFileOutputFormat.class );		
				break;
		}
	
		FileInputFormat.addInputPath( job, this.input );
		FileOutputFormat.setOutputPath( job, this.input.suffix( "_transl" ) );
		
		if ( !job.waitForCompletion( true ) )
			return 1;
	
		return 0;
	}
}