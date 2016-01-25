/**
 *	@file InitializationMapperCluster.java
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import pad.UtilCounters;

/**	Mapper task of the \see InitializationDriver Job. */
public class InitializationMapperCluster extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
{
	// Minus one indicates that a node is alone.
	private static final IntWritable MINUS_ONE = new IntWritable( -1 );
	private IntWritable nodeID = new IntWritable();
	private IntWritable neighborID = new IntWritable();
	private MultipleOutputs<IntWritable, IntWritable> mos = null;
	
	/**
	 * Setup method of the this InitializationMapperCluster class.
	 * Set up the multiple outputs variable, used to write the "real" result into the special folder.
	 * @param context	context of this Job.
	 * @throws IOException, InterruptedException
	 */
	protected void setup( Context context ) throws IOException, InterruptedException
	{
		this.mos = new MultipleOutputs<IntWritable, IntWritable>( context );
	}
	
	/**
	 * Map method of the this InitializationMapperCluster class.
	 * Each line has the following format: NodeID1<SPACE>NodeID2<SPACE>NodeID3....
	 * This means that all the nodes in the line are strongly connected to each others.
	 * In this case, we read a line and we split it by the <SPACE> character.
	 * Then, we produce all the combination between two nodes found in the set and we
	 * emit the pair <NodeID, NeighborID> if NodeID > NeighborID.
	 * We store this result into the special folder.
	 * In the regular folder we emit all the encountered nodes.
	 * @param _			offset of the line read, not used in this method.
	 * @param value		text of the line read.
	 * @param context	context of this Job.
	 * @throws IOException, InterruptedException
	 */
	public void map( LongWritable _, Text value, Context context ) throws IOException, InterruptedException 
	{
		// Read line.
		String line = value.toString();
		
		// Increment the number of initial clusters, since in each line there is a new cluster.
		context.getCounter( UtilCounters.NUM_INITIAL_CLUSTERS ).increment( 1 );

		// Split the line on the space character.
		String clusterLists[] = line.split( " " );
		
		// If the node is alone.
		if ( clusterLists.length == 1 )
		{
			// Extract the nodeID.
			nodeID.set( Integer.parseInt( clusterLists[0] ) );
			// Emit the node.
			context.write( nodeID, MINUS_ONE );
			// Emit the node in the special folder.
			mos.write( nodeID, MINUS_ONE, pad.InitializationDriver.MOS_BASEOUTPUTPATH );
			return;
		}
		
		// The input file is format as cluster list.
		// We produce all the combination between two nodes found in the set.
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
				// Emit the pair in the special folder.
				mos.write( nodeID, neighborID, pad.InitializationDriver.MOS_BASEOUTPUTPATH );
			}
			
			// Emit the encountered node.
			nodeID.set( nodeX );
			context.write( nodeID, MINUS_ONE );
		}
		// Emit the encountered node.
		nodeID.set( Integer.parseInt( clusterLists[clusterLists.length - 1] ) );
		context.write( nodeID, MINUS_ONE );
	}
	
	/**
	 * Cleanup method of the this InitializationMapperCluster class.
	 * Close the multiple output file.
	 * @param context	context of this Job.
	 * @throws IOException, InterruptedException
	 */
	protected void cleanup( Context context ) throws IOException, InterruptedException
	{
		this.mos.close();
	}
}