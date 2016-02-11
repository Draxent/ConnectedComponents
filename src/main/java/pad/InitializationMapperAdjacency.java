/**
 *	@file InitializationMapperAdjacent.java
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

import pad.UtilCounters;

/**	Mapper task of the \see InitializationDriver Job. */
public class InitializationMapperAdjacency extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
{
	public static final IntWritable MINUS_ONE = new IntWritable( -1 );
	private IntWritable nodeID = new IntWritable();
	private IntWritable neighbourID = new IntWritable();
	
	/**
	* Map method of the this InitializationMapperAdjacent class.
	* Each line has the following format: NodeID<TAB>NeighborID1,NeighborID2,...
	* So we read a line and we split it by the <TAB> character, and the second part by the comma character.
	* Then, for each neighbor, we produce the pair <NodeID, NeighborID>
	* if NodeID > NeighborID since it is the connection that we need in the following operations.
	* @param _			offset of the line read, not used in this method.
	* @param value		text of the line read.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void map( LongWritable _, Text value, Context context ) throws IOException, InterruptedException 
	{
		// Read line.
		String line = value.toString();
		
		// Increment the number of nodes, since the input file presents a new node in each line.
		context.getCounter( UtilCounters.NUM_INITIAL_NODES ).increment( 1 );

		// Split the line on the tab character.
		String userID_neighbourhood[] = line.split( "\t" );
		
		// Extract the nodeID.
		nodeID.set( Integer.parseInt( userID_neighbourhood[0] ) );
		
		// If the node is alone.
		if ( userID_neighbourhood.length == 1 )
		{
			// NeighbourID is set to minus one, to indicate that the node is alone.
			context.write( nodeID, MINUS_ONE );
			return;
		}
		
		// Split by "," to find the list of neighbours of nodeID.
		String neighbours[] = userID_neighbourhood[1].split( "," );
		
		// Emit the pair <nodeID, neighbourID> for each neighbours.
		for ( int i = 0; i < neighbours.length; i++ )
		{
			neighbourID.set( Integer.parseInt( neighbours[i] ) );
			// only if nodeID > neighbourID
			if ( nodeID.get() > neighbourID.get() )
				context.write( nodeID, neighbourID );
		}
	}
}
