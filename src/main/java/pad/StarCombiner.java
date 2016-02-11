/**
 *	@file StarCombiner.java
 *	@brief Combiner task of the \see StarDriver Job.
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
import org.apache.hadoop.mapreduce.Reducer;

/** Combiner task of the \see StarDriver Job. */
public class StarCombiner extends Reducer<NodesPairWritable, IntWritable, NodesPairWritable, IntWritable> 
{	
	/**
	* Reduce method of the this StarCombiner class.
	* It reduce the number of duplicates that are emit by the \see StarMapper.
	* @param pair			pair of nodes.
	* @param neighbourhood	list of neighbours.
	* @param context		context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void reduce( NodesPairWritable pair, Iterable<IntWritable> neighbourhood, Context context ) throws IOException, InterruptedException 
	{
		// Do not exists a node with ID equal to minus two ( minus one already used to indicate loneliness )
		int lastNodeSeen = -2;
		for ( IntWritable neighbour : neighbourhood )
		{
			// Skip the duplicate nodes.
			if ( neighbour.get() == lastNodeSeen )
				continue;
			
			// Emit the pair
			pair.NeighbourID = neighbour.get();
			context.write( pair, neighbour );
			
			// Store the last neighbourId that we have processed.
			lastNodeSeen = neighbour.get();
		}
	}
}
