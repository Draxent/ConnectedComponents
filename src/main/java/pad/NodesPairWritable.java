/**
 *	@file NodesPairWritable.java
 *	@brief Data structure used to wrap two nodes into a key; useful also to implement the secondary sort.
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

/**	Data structure used to wrap two nodes into a key; useful also to implement the secondary sort. */
public class NodesPairWritable implements WritableComparable<NodesPairWritable>
{
	/**	Identifier of the node */
	public Integer NodeID = new Integer( -1 );
	/**	Identifier of the neighbor node. The default value ( minus one) means that NodeID has no neighbors. */
	public Integer NeighborID = new Integer( -1 );
	
	/**
	* Deserializes the array. Read the data out in the order it is written.
	* @param in		source for raw byte representation.
	* @throws IOException
	*/
	public void readFields( DataInput in ) throws IOException
	{
		this.NodeID = in.readInt();
		this.NeighborID = in.readInt();
	}
	
	/**
	* Serializes this array. Write the data out in the order it is read.
	* @param out	where to write the raw byte representation.
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
	public int compareTo( NodesPairWritable other )
	{
		int result = this.NodeID - other.NodeID;
		if( result == 0 )
			result = this.NeighborID - other.NeighborID;
		return result;
	}
	
	/**
	* Calculate hash code of this object.
	* @return 		the hash code.
	*/
    public int hashCode()
    {
    	int hash1 = (this.NodeID != null) ? this.NodeID.hashCode() : 0;
    	int hash2 = (this.NeighborID != null) ? this.NeighborID.hashCode() : 0;
    	return (hash1 + hash2) * hash2 + hash1;
    }
    
	/**
	* Check if two objects that are instance of \see NodesPairWritable are equals.
	* @param other	the other object with which to make comparisons.
	* @return 		<c>true</c> if the two objects are equals, <c>false</c> otherwise.
	*/
    public boolean equals( Object other )
    {
    	if ( this == other ) return true;
    	if ( !(other instanceof NodesPairWritable) ) return false;
    	
    	NodesPairWritable pair = (NodesPairWritable) other;
		boolean cond1 = ( this.NodeID != null && pair.NodeID != null && this.NodeID.equals(pair.NodeID) );
		boolean cond2 = ( this.NeighborID != null && pair.NeighborID != null && this.NeighborID.equals(pair.NeighborID) ); 
		return cond1 && cond2;
    }
}