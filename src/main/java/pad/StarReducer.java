/**
 *	@file StarReducer.java
 *	@brief Reducer task of the \see StarDriver Job.
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

import pad.UtilCounters;

/** Reducer task of the \see StarDriver Job. */
public class StarReducer extends Reducer<NodesPairWritable, IntWritable, IntWritable, IntWritable> 
{
	private static final IntWritable MINUS_ONE = new IntWritable( -1 );
	private IntWritable nodeID = new IntWritable();
	private IntWritable minNodeID = new IntWritable();
	private boolean smallStar;
	
	/**
	* Setup method of the this StarReducer class.
	* Extract the <em>type</em> variable from the context configuration.
	* Based on this value, this Reducer will behave as a Small-Star Reducer or Large-Star Reducer.
	* @param context	context of this Job.
	*/
	public void setup( Context context )
	{
		smallStar = context.getConfiguration().get( "type" ).equals( "SMALL" );
	}
	
	/**
	* Reduce method of the this StarReducer class.
	* Since the neighbors are sorted, thanks to the secondary sort, we know that the
	* minimum node is either the NodeID or the first neighbor. We call <em>MinNodeID</em> this node.
	* For each neighbor, we produce the pairs <NeighborID, MinNodeID> and <MinNodeID, NeighborID> :
	* 	-	always, if it is a Small-Star Reducer;
	*   -	only when NeighborID is greater than NodeID, if it is a Large-Star Reducer.
	* @param pair			pair used to implement the secondary sort, \see NodesPair.
	* @param neighborhood	list of neighbors.
	* @param context		context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void reduce( NodesPairWritable pair, Iterable<IntWritable> neighborhood, Context context ) throws IOException, InterruptedException 
	{
		long numProducedPairs = 0;
		
		// This means that the nodeID is isolated, so we emit it unchanged
		if ( pair.NeighborID == -1 )
		{
			minNodeID.set( pair.NodeID );
			context.write( minNodeID, MINUS_ONE );
			return;			
		}
		
		// Thanks to the secondary sorting, we know the the first element contains
		// the neighbor node with the minimum label. We just need to compare it with the node id.
		minNodeID.set( Math.min( pair.NodeID, pair.NeighborID ) );
		
		// If we are running Small-Star, we need to connect this node to the minimum neighbors
		if ( smallStar && ( pair.NodeID != minNodeID.get() ) )
		{
			nodeID.set( pair.NodeID );
			context.write( nodeID, minNodeID );		
		}
		
		// Do not exists a node with ID equal to minus two ( minus one already used to indicate loneliness )
		int lastNodeSeen = -2;
		for ( IntWritable neighbor : neighborhood )
		{
			// Skip the duplicate nodes.
			if ( neighbor.get() == lastNodeSeen )
				continue;
			
			// If we are running Small-Star, we always emit the neighbors except when it is the minNodeID
			// If we are running Large-Star, we emit only when the neighborID is greater than nodeID
			boolean cond = ( smallStar ? ( neighbor.get() != minNodeID.get() ) : ( neighbor.get() > pair.NodeID ) );
			
			if ( cond )
			{
				context.write( neighbor, minNodeID );
				numProducedPairs++;
			}
			
			// Store the last neighborId that we have processed.
			lastNodeSeen = neighbor.get();
		}
		
		// If the NodeID has not the minimum label means that the produced pairs will be different,
		// so we increment the number of changes by the number of produced pairs
		if ( pair.NodeID != minNodeID.get() )
			context.getCounter( UtilCounters.NUM_CHANGES ).increment( numProducedPairs );
	}
}
