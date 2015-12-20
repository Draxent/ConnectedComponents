/**
 *	@file InitializationMapper.java
 *	@brief Mapper task of the \see InitializationDriver Job.
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**	Mapper task of the \see InitializationDriver Job. */
public class InitializationMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
{
	public static final IntWritable MINUS_ONE = new IntWritable( -1 );
	private IntWritable nodeID = new IntWritable();
	private IntWritable neighborID = new IntWritable();
	
	/**
	* Map method of the this InitializationMapper class.
	* The input file can be format as an adjacent list or a cluster list.
	* 
	* If it is an adjacent list, each line has the following format: NodeID<TAB>NeighborID1,NeighborID2,...
	* So we read a line and we split it by the <TAB> character, and the second part by the comma character.
	* Then, for each neighbor, we produce the pair <NodeID, NeighborID>
	* if NodeID > NeighborID since it is the connection that we need in the following operations.
	* 
	* If it is a cluster list, each line has the following format: NodeID1<SPACE>NodeID2<SPACE>NodeID3....
	* This means that all the nodes in the line are strongly connected to each others.
	* In this case, we read a line and we split it by the <SPACE> character.
	* Then, we produce all the combination between two nodes found in the set and we
	* emit the pair <NodeID, NeighborID> if NodeID > NeighborID.
	* 
	* @param _			offset of the line read, not used in this method.
	* @param value		text of the line read.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void map( LongWritable _, Text value, Context context ) throws IOException, InterruptedException 
	{
		// Read line.
		String line = value.toString();

		// Split the line on the tab character.
		String userID_neighborhood[] = line.split( "\t" );
		
		// If <TAB> not found, the format of input file can be cluster format or the node is alone.
		if( userID_neighborhood.length == 1 )
		{
			// Split the line on the space character.
			String clusterLists[] = line.split( " " );
			
			// If the node is alone.
			if ( clusterLists.length == 1 )
			{
				// Extract the nodeID.
				nodeID.set( Integer.parseInt( userID_neighborhood[0] ) );
				// NeighborID is set to minus one, to indicate that the node is alone.
				context.write( nodeID, MINUS_ONE );
				return;
			}
			
			// The input file is format as cluster list.
			// We produce all the combination between two nodes found in the set
			for ( int i = 0; i < clusterLists.length - 1; i++ )
			{
				// Extract the nodeID.
				int nodeX = Integer.parseInt( clusterLists[i] );
				
				for ( int j = i + 1; j < clusterLists.length; j++ )
				{
					// Extract the neighborID.
					int nodeY = Integer.parseInt( clusterLists[j] );
					
					nodeID.set( Math.max ( nodeX, nodeY ) );
					neighborID.set( Math.min ( nodeX, nodeY ) );
					context.write( nodeID, neighborID );
				}
			}		
			
		}
		// The input file is format as adjacent list.
		else
		{
			// Extract the nodeID.
			nodeID.set( Integer.parseInt( userID_neighborhood[0] ) );
			
			// Split by "," to find the list of neighbors of nodeID.
			String neighbors[] = userID_neighborhood[1].split( "," );
			
			// Emit the pair <nodeID, neighborID> for each neighbors.
			for ( int i = 0; i < neighbors.length; i++ )
			{
				neighborID.set( Integer.parseInt( neighbors[i] ) );
				// only if nodeID > neighborID
				if ( nodeID.get() > neighborID.get() )
					context.write( nodeID, neighborID );
			}
		}
	}
}