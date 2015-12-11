/**
 *	@file LargeStartMapper.java
 *	@brief Mapper task of the \ref StarDriver Job.
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

/** Mapper task of the \ref StarDriver Job. */
public class StarMapper extends Mapper<LongWritable, Text, NodesPair, IntWritable> 
{
	public static final Logger LOG = Logger.getLogger( StarMapper.class );
	private boolean smallStar;
	private NodesPair pair = new NodesPair();
	private IntWritable neighbourID = new IntWritable();

	/**
	* Setup method of the this StarMapper class.
	* Extract the <em>type</em> variable from the context configuration.
	* Based on this value, this Mapper will behave as a Small-Star Mapper or Large-Star Mapper.
	* @param context	context of this Job.
	*/
	public void setup( Context context )
	{
		LOG.setLevel( Level.ERROR );
		smallStar = context.getConfiguration().get( "type" ).equals( "SMALL" );
	}
	
	/**
	* Map method of the this StarMapper class.
	* Extract the <nodeID, NeighborID> from the line.
	* If it is a Large-Star Mapper, it always emits the pair read.
	* If it is a Small-Star Mapper, it emits the pair read only when NeighborID is smaller than NodeID.
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
		String nodeID_neighbourID[] = line.split( "\t" );
		
		// Extract the nodeID.
		pair.NodeID = Integer.parseInt( nodeID_neighbourID[0] );
		
		// Extract the neighbourID.
		pair.NeighborID =  Integer.parseInt( nodeID_neighbourID[1] );
		neighbourID.set( pair.NeighborID );
		
		// If we are running Small-Star, we emit only when the neighborID is smaller than nodeID
		// If we are running Large-Star, we always emit the neighbors
		boolean cond = ( smallStar ? ( pair.NeighborID < pair.NodeID ) : true );

		// Emit < (u, v) ; v >
		if ( cond )
			context.write( pair, neighbourID );
	}
}