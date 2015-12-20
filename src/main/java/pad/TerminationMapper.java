/**
 *	@file TerminationMapper.java
 *	@brief Mapper task of the \see TerminationDriver Job.
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
import org.apache.hadoop.mapreduce.Mapper;

/**	Mapper task of the \see TerminationDriver Job. */
public class TerminationMapper extends Mapper<IntWritable, IntWritable, NodesPairWritable, IntWritable> 
{
	private NodesPairWritable pair = new NodesPairWritable();

	/**
	* Map method of the this TerminationMapper class.
	* Emits the pair <min(u,v), max(u,v)>. In this way the \see TerminationReducer will receive a cluster for each key.
	* @param nodeID			identifier of the node.
	* @param neighborID		identifier of the neighbor.
	* @param context		context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void map( IntWritable nodeID, IntWritable neighborID, Context context ) throws IOException, InterruptedException 
	{
		// if the label of node is less than the label of the neighbor
		if ( nodeID.get() < neighborID.get() || neighborID.get() == -1 )
		{
			// Set up the pair.
			pair.NodeID = nodeID.get();
			pair.NeighborID =  neighborID.get();
			
			context.write( pair, neighborID );
		}
		else
		{
			// Set up the pair.
			pair.NodeID = neighborID.get();
			pair.NeighborID =  nodeID.get();
			
			context.write( pair, nodeID );
		}
	}
}