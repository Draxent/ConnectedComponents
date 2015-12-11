/**
 *	@file TerminationReducer.java
 *	@brief Reducer task of the \ref TerminationDriver Job.
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import pad.TerminationDriver.UtilCounters;

/**	Reducer task of the \ref TerminationDriver Job. */
public class TerminationReducer extends Reducer<NodesPair, IntWritable, IntWritable, NullWritable> 
{
	public static final Logger LOG = Logger.getLogger( TerminationReducer.class );
	private static final String BASE_NAME = "cluster_";
	private static final NullWritable NULL = NullWritable.get();
	private IntWritable nodeID = new IntWritable();
	private MultipleOutputs<IntWritable, NullWritable> mos;
	
	/**
	* Setup method of the this TerminationReducer class.
	* Initialize the MultipleOutputs object used to write the nodes of each cluster found in a distinct file.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	protected void setup( Context context ) throws IOException, InterruptedException
	{
		LOG.setLevel( Level.ERROR );
		mos = new MultipleOutputs<IntWritable, NullWritable>( context );
	}
	
	/**
	* Reduce method of the this TerminationReducer class.
	* For each NodeID, we emit this node and all its neighbors in a distinct file.
	* Than we increment the NUM_CLUSTERS of \see pad.TerminationDriver.UtilCounters by one.
	* @param pair			pair used to implement the secondary sort, \see NodesPair.
	* @param neighborhood	list of neighbors.
	* @param context		context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void reduce( NodesPair pair, Iterable<IntWritable> neighborhood, Context context ) throws IOException, InterruptedException 
	{
		String baseOutputPath = BASE_NAME + pair.NodeID;
		
		// The cluster is surely composed by this node that is also the minimum label node
		// thanks to the convergence properties of Small-Star and Large-Star. 
		nodeID.set( pair.NodeID  );
		mos.write( nodeID, NULL, baseOutputPath );
		
		// If the node is not alone
		if ( pair.NeighborID != -1 )
		{
			// Add to the cluster all the neighbors of the node,
			// we know that the neighbors are sort in ascending order
			for ( IntWritable neighbor : neighborhood )
				mos.write( neighbor, NULL, baseOutputPath );
		}
		
		// Increment the number of clusters
		context.getCounter( UtilCounters.NUM_CLUSTERS ).increment( 1 );
	}
	
	/**
	* Cleanup method of the this TerminationReducer class.
	* Close the MultipleOutputs object.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	protected void cleanup( Context context ) throws IOException, InterruptedException
	{
		mos.close();
	}
}