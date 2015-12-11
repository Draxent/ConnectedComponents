/**
 *	@file InitializationMapper.java
 *	@brief Mapper task of the \ref InitializationDriver Job.
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import pad.InitializationDriver.UtilCounters;

/**	Mapper task of the \ref InitializationDriver Job. */
public class InitializationMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public static final Logger LOG = Logger.getLogger( InitializationMapper.class );
	private static final Text MINUS_ONE = new Text( "-1" );
	private Text nodeID = new Text();
	private Text neighbourID = new Text();

	public void setup( Context context )
	{
		LOG.setLevel( Level.ERROR );
	}
	
	/**
	* Map method of the this InitializationMapper class.
	* Each line has the following format: NodeID<TAB>NeighborID1,NeighborID2,...
	* So we read a line and we split it by the <TAB> character, and the second part by the comma character.
	* Then, for each neighbor, we produce the pair <NodeID, NeighborID>.
	* Since each line present a unique NodeID, we increment the NUM_NODES of
	* \see pad.InitializationDriver.UtilCounters by one for each line encountered.
	* @param _			offset of the line read, not used in this method.
	* @param value		text of the line read.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void map( LongWritable _, Text value, Context context ) throws IOException, InterruptedException 
	{
		// Read line.
		String line = value.toString();
		// Increment the number of nodes ( since the input file presents a new node in each line )
		context.getCounter( UtilCounters.NUM_NODES ).increment( 1 );
		// Split the line on the tab character.
		String userID_neighborhood[] = line.split( "\t" );
		
		// Extract the nodeID.
		nodeID.set( userID_neighborhood[0] );
		
		// If the node is alone
		if( userID_neighborhood.length == 1 )
		{
			// Emit this nodeID with neighbor -1, in order to keep this information.
			context.write( nodeID, MINUS_ONE );
			return;
		}
		
		// Split by "," to find the list of neighbors of nodeID.
		String neighbors[] = userID_neighborhood[1].split( "," );
		int length = neighbors.length;
		
		// Emit the pair < nodeID, neighborID > for each neighbors
		for ( int i = 0; i < length; i++ )
		{
			neighbourID.set( neighbors[i] );
			context.write( nodeID, neighbourID );
		}
	}
}