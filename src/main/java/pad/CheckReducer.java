/**
 *	@file CheckReducer.java
 *	@brief Reducer task of the \ref CheckDriver Job.
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import pad.CheckDriver.UtilCounters;

/**	Reducer task of the \ref CheckDriver Job. */
public class CheckReducer extends Reducer<IntWritable, NullWritable, NullWritable, NullWritable> 
{
	public static final Logger LOG = Logger.getLogger( CheckMapper.class );
	
	public void setup( Context context )
	{
		LOG.setLevel( Level.ERROR );
	}
	
	/**
	* Reduce method of the this CheckReducer class.
	* Counts the number of <em>values</em> present for a given key, and if they are more than one,
	* increment the NUM_ERRORS of \see pad.CheckDriver.UtilCounters by one.
	* @param nodeID		node identifier.
	* @param values		if there is more then one value for key, the cluster is malformed.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void reduce( IntWritable nodeID, Iterable<NullWritable> values, Context context ) throws IOException, InterruptedException 
	{
		// Count the number of times the nodeID is present inside the various clusters
		int num_values = 0;
		Iterator<NullWritable> iter = values.iterator();
		while ( iter.hasNext() )
		{
			iter.next();
			num_values++;
		}
		
		// If it is present more than one time, it is an error, i.e. the cluster are malformed.
		if ( num_values > 1 )
			context.getCounter( UtilCounters.NUM_ERRORS ).increment( 1 );
	}
}