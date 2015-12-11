/**
 *	@file NodesPair.java
 *	@brief Data structure used to wrap two nodes into a key, in order to implement the secondary sort.
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**	Data structure used to wrap two nodes into a key, in order to implement the secondary sort. */
public class NodesPair implements WritableComparable<NodesPair>
{
	/**	identifier of the node */
	public int NodeID = -1;
	/**	identifier of the neighbor node */
	public int NeighborID = -1;
	
	/**
	* Read the data out in the order it is written.
	* @param in		input data.
	* @throws IOException
	*/
	public void readFields( DataInput in ) throws IOException
	{
		this.NodeID = in.readInt();
		this.NeighborID = in.readInt();
	}
	
	/**
	* Write the data out in the order it is read.
	* @param out	output data.
	* @throws IOException
	*/
	public void write( DataOutput out ) throws IOException
	{
		out.writeInt( this.NodeID );
		out.writeInt( this.NeighborID );	
	}
	
	/**
	* Convert the object into a string.
	* @return	the resulting string.
	*/
	public String toString()
	{
		return this.NodeID + "\t" + this.NeighborID;
	}

	/**
	* Compare this object with other one of its kind.
	* It compare first the two objects looking to the NodeID.
	* If they have the same NodeID, compare them looking to the NeighborID.
	* @param other	the other object with which to make comparisons.
	* @return 		<c>0</c> if the two objects are identical,
	* 				<c>-1</c> if this object is smaller than the <em>other</em>.
	* 				<c>1</c> if this object is greater than the <em>other</em>.
	*/
	public int compareTo( NodesPair other )
	{
		int result = this.NodeID - other.NodeID;
		if( result == 0 )
			result = this.NeighborID - other.NeighborID;
		return result;
	}
}