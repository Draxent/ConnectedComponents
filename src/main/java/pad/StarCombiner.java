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
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/** Combiner task of the \see StarDriver Job. */
public class StarCombiner extends Reducer<NodesPairWritable, IntWritable, NodesPairWritable, IntWritable> 
{	
	private static final int MEMORY_THRESHOLD = 80;
	private static final int CHECKS_EVERY_X_NUMBERS = 100000;
	private HashSet<Integer> seenNeighbor = new HashSet<Integer>();
	
	/**
	* Reduce method of the this StarCombiner class.
	* It reduce the number of duplicates that are emit by the \see StarMapper.
	* The secondary sort is not yet available, so we need an HashSet structure in order to verify
	* whether or not we have already seen a neighbor.
	* @param pair			pair of nodes.
	* @param neighborhood	list of neighbors.
	* @param context		context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void reduce( NodesPairWritable pair, Iterable<IntWritable> neighborhood, Context context ) throws IOException, InterruptedException 
	{
		int count = 0;
		seenNeighbor.clear();
		
		for ( IntWritable neighbor : neighborhood )
		{
			pair.NeighborID = neighbor.get();
					
			// Skip the duplicate nodes.
			if ( seenNeighbor.contains( pair.NeighborID ) )
				continue;
			
			context.write( pair, neighbor );
			
			// Check the state of the memory every CHECKS_EVERY_X_NUMBERS.
			if ( count % CHECKS_EVERY_X_NUMBERS == 0 )
			{
				checkMemory();
				count = 0;
			}
			
			// Store the neighborId into the HashTable
			seenNeighbor.add( pair.NeighborID );
			
			// Count the number of neighbors added.
			count++;
		}
	}
	
	/**
	* If the memory is starting to become a concern, free it.
	* Even if we allowed some duplicates to go on, the logic is not altered, and we deal with
	* duplicates neighbors in the reduce phase.
	*/
	private void checkMemory()
	{
		final long totalMemory = Runtime.getRuntime().totalMemory();
		final long freeMemory = Runtime.getRuntime().freeMemory();
		final float percMemUsed = ((float) (totalMemory - freeMemory) / totalMemory) * 100;
		if ( percMemUsed > MEMORY_THRESHOLD )
			seenNeighbor.clear();
	}
}
