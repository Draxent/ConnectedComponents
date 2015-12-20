/**
 *	@file ClusterWritable.java
 *	@brief Data structure used to write a cluster on hdfs files.
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
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

/**	Data structure used to write a cluster on hdfs files. */
public class ClusterWritable extends ArrayList<Integer> implements Writable
{
	private static final long serialVersionUID = 1L;
	
	/**	Array of nodes that make up the cluster */
	public ArrayList<Integer> Cluster = new ArrayList<Integer>();
	
	/** Creates an ClusterWritable object. */
	public ClusterWritable()
	{
		super();
	}

	/** Creates an ClusterWritable object from an ArrayList. */
	public ClusterWritable( ArrayList<Integer> array )
	{
		super( array );
	}
	
	/**
	* Deserializes the array. Read the data out in the order it is written.
	* @param in		source for raw byte representation.
	* @throws IOException
	*/
	public void readFields( DataInput in ) throws IOException
	{
		this.clear();

		int numFields = in.readInt();
		if ( numFields == 0 ) return;
		
		for (int i = 0; i < numFields; i++)
			this.add( in.readInt() );
	}
	
	/**
	* Serializes this array. Write the data out in the order it is read.
	* @param out	where to write the raw byte representation.
	* @throws IOException
	*/
	public void write( DataOutput out ) throws IOException
	{
		out.writeInt( this.size() );
		if ( size() == 0 ) return;
		
		for ( int i = 0; i < size(); i++ )
			out.writeInt( this.get( i ) );
	}
	
	/**
	* Convert the object into a string.
	* @return	the resulting string.
	*/
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		
		if ( this.size() > 0 )
		{
			sb.append( this.get(0) );
			for ( int i = 1; i < this.size(); i++ )
				sb.append( " " ).append( this.get(i) );
		}
		return sb.toString();
	}
}