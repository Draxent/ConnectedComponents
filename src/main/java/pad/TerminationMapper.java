/**
 *	@file TerminationMapper.java
 *	@brief Mapper task of the \ref TerminationDriver Job.
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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**	Mapper task of the \ref TerminationDriver Job. */
public class TerminationMapper extends Mapper<LongWritable, Text, NodesPair, IntWritable> 
{
	public static final Logger LOG = Logger.getLogger( TerminationMapper.class );
	private NodesPair pair = new NodesPair();
	private IntWritable neighborID = new IntWritable();

	public void setup( Context context )
	{
		LOG.setLevel( Level.ERROR );
	}
	
	/**
	* Map method of the this TerminationMapper class.
	* Extract the <nodeID, NeighborID> from the line and emit it only if nodeID is smaller than neighborID.
	* In this way we pass from a bidirectional link to a unidirectional one.
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
		String userID_neighbor[] = line.split( "\t" );
		
		// Extract the nodeID and neighbourID.
		pair.NodeID = Integer.parseInt( userID_neighbor[0] );
		pair.NeighborID = Integer.parseInt( userID_neighbor[1] );
		neighborID.set( pair.NeighborID );
		
		// Emit the pair of nodes only if the nodeID is isolated or nodeID < neighbourID,
		// in this way we pass from a bidirectional link to a unidirectional link.
		if ( (pair.NeighborID == -1) || (pair.NodeID < pair.NeighborID) )
			context.write( pair, neighborID );
	}
}