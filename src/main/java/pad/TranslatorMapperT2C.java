/**
 *	@file TranslatorMapperT2C.java
 *	@brief Mapper of \see TranslatorDriver.
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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Mapper of \see TranslatorDriver. */
public class TranslatorMapperT2C extends Mapper<LongWritable, Text, ClusterWritable, NullWritable> 
{
	private static final NullWritable NULL = NullWritable.get();
	private ClusterWritable cluster = new ClusterWritable();
	
	/**
	* Map method of the this TranslatorMapperT2C class.
	* Extract the Cluster from the line and emit it.
	* @param _			offset of the line read, not used in this method.
	* @param value		text of the line read.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void map( LongWritable _, Text value, Context context ) throws IOException, InterruptedException
	{
		// Clear the cluster
		cluster.clear();
		
		// Read line.
		String line = value.toString();
		
		// Split the line on the space character.
		String nodes[] = line.split( " " );
		
		// Extract the cluster elements
		for ( int i = 0; i < nodes.length; i++ )
			cluster.add( Integer.parseInt( nodes[i] ) );
		
		// Emit the cluster
		context.write( cluster, NULL );
	}
}