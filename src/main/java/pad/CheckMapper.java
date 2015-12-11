/**
 *	@file CheckMapper.java
 *	@brief Mapper task of the \ref CheckDriver Job.
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

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**	Mapper task of the \ref CheckDriver Job. */
public class CheckMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> 
{
	private static final NullWritable NULL = NullWritable.get();
	public static final Logger LOG = Logger.getLogger( CheckMapper.class );
	private IntWritable nodeID = new IntWritable();
	
	public void setup( Context context )
	{
		LOG.setLevel( Level.ERROR );
	}

	/**
	* Map method of the this CheckMapper class.
	* Read the node value, present in each line; transform it in a IntWritable and write it into the context.
	* @param _			offset of the line read, not used in this method.
	* @param value		text of the line read.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void map( LongWritable _, Text value, Context context ) throws IOException, InterruptedException 
	{
		// Read line.
		String line = value.toString();

		// Extract the nodeID.
		nodeID.set( Integer.parseInt( line ) );
		
		// Write the nodeID, as key, in the context.
		context.write( nodeID, NULL );
	}
}