/**
 *	@file LargeStarPartitioner.java
 *	@brief Thanks to this class, the keys (\see NodesPair) are partitioned only considering the first component, i.e NodeID.
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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**	Thanks to this class, the keys (\see NodesPair) are partitioned only considering the first component, i.e NodeID. */
public class NodePartitioner extends Partitioner<NodesPair, IntWritable>
{
	/**
	* Choose the Reducer identifier to which send the record using only the NodeID information.
	* @param pair			key of the record, \see NodesPair.
	* @param _				value of the record.
	* @param numPartitions	number of Reducer used.
	* @return 				Reducer identifier to which send this record.
	*/
	public int getPartition( NodesPair pair, IntWritable _, int numPartitions )
	{
		return pair.NodeID % numPartitions;
	}
}