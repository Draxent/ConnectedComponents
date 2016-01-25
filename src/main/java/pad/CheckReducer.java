/**
 *	@file CheckReducer.java
 *	@brief Reducer task of the \see CheckDriver Job.
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
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import pad.UtilCounters;

/**	Reducer task of the \see CheckDriver Job. */
public class CheckReducer extends Reducer<IntWritable, NullWritable, NullWritable, NullWritable> 
{
	/**
	* Reduce method of the this CheckReducer class.
	* Counts the number of <em>values</em> present for a given key and, if they are more than one,
	* increment the NUM_ERRORS of \see UtilCounters by one.
	* @param nodeID		node identifier.
	* @param values		if there is more then one value for key, at least one cluster is malformed.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void reduce( IntWritable nodeID, Iterable<NullWritable> values, Context context ) throws IOException, InterruptedException 
	{
		// Count the number of times the nodeID is present inside the various clusters
		int count = 0;
		
		Iterator<NullWritable> iter = values.iterator();
		while ( iter.hasNext() )
		{
			iter.next();
			count++;
			
			// If it is present more than one time, it is an error, i.e. at least one cluster is malformed.
			if ( count > 1 )
			{
				context.getCounter( UtilCounters.NUM_ERRORS ).increment( 1 );
				break;
			}
		}
	}
}