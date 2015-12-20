/**
 *	@file NodeGroupingComparator.java
 *	@brief Thanks to this class, the reducer bundles together records with the same
 *		   NodeID while it is streaming the mapper output records from local disk.
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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Thanks to this class, the reducer bundles together records with the same
 * NodeID while it is streaming the mapper output records from local disk.
 */
public class NodeGroupingComparator extends WritableComparator
{
	/** Initializes a new instance of the NodeGroupingComparator class. */
	protected NodeGroupingComparator()
	{
		super( NodesPairWritable.class, true );
	}
	
	/**
	* Compare two keys read from the mapper output records,
	* only looking  to the first component, i.e NodeID.
	* @param key1	first key.
	* @param key2	second key.
	* @return 		<c>0</c> if the NodeID is the same,
	* 				<c>-1</c> if key1 is smaller than key2
	* 				<c>1</c> if key1 is greater than key2.
	*/
	@SuppressWarnings("rawtypes")
	public int compare( WritableComparable key1, WritableComparable key2 )
	{
		NodesPairWritable pair1 = (NodesPairWritable)key1;
		NodesPairWritable pair2 = (NodesPairWritable)key2;
		
		return pair1.NodeID - pair2.NodeID;
	}
}