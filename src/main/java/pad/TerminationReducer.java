/**
 *	@file TerminationReducer.java
 *	@brief Reducer task of the \see TerminationDriver Job.
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import pad.UtilCounters;

/**	Reducer task of the \see TerminationDriver Job. */
public class TerminationReducer extends Reducer<NodesPairWritable, IntWritable, ClusterWritable, NullWritable> 
{
	private static final NullWritable NULL = NullWritable.get();
	private ClusterWritable cluster = new ClusterWritable();
	
	/**
	* Reduce method of the this TerminationReducer class.
	* For each NodeID, we add that node and all its neighbours to the ClusterWritable object; than we emit it.
	* Than we increment the NUM_CLUSTERS of \see pad.TerminationDriver.UtilCounters by one.
	* @param pair			pair used to implement the secondary sort, \see NodesPair.
	* @param neighbourhood	list of neighbours.
	* @param context		context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void reduce( NodesPairWritable pair, Iterable<IntWritable> neighbourhood, Context context ) throws IOException, InterruptedException 
	{
		// Clear the cluster. We have a distinct cluster for each key.
		cluster.clear();

		// The cluster is surely composed by this node that is also the minimum label node
		// thanks to the convergence properties of Small-Star and Large-Star. 
		cluster.add( pair.NodeID );
		
		// If the node is not alone
		if ( pair.NeighbourID != -1 )
		{
			// Do not exists a node with ID equal to minus two ( minus one already used to indicate loneliness )
			int lastNodeSeen = -2;
			
			// Add to the cluster all the neighbours of the node,
			// we know that the neighbours are sort in ascending order thanks to the secondary order.
			for ( IntWritable neighbour : neighbourhood )
			{
				// Skip the duplicate nodes.
				if ( neighbour.get() == lastNodeSeen )
					continue;
				
				// Add the node to the cluster
				cluster.add( neighbour.get() );
				
				// Store the last neighbourId that we have processed.
				lastNodeSeen = neighbour.get();
			}
		}
		
		// Increment the number of nodes by the number of nodes of this cluster.
		context.getCounter( UtilCounters.NUM_NODES ).increment( cluster.size() );
		// Increment the number of clusters by one.
		context.getCounter( UtilCounters.NUM_CLUSTERS ).increment( 1 );
		// Emit the cluster.
		context.write( cluster, NULL );
	}
}
