/**
 *	@file Cluster.java
 *	@brief Data structure that manages the hdfs file where the Cluster nodes are stored.
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Data structure that manages the hdfs file where the Cluster nodes are stored 
 * and implements some basic operations on that. It is presented to the user as an \see Iterator,
 * so that the user can iterate all the nodes of the file.
 * This is the most efficient way to look up the content of the cluster.
 * The other methods are very expensive, so use them carefully.
 */
public class Cluster implements Iterator<Integer>
{
	private int id;
	private Path path;
	private FileSystem fs;
	private BufferedReader buffer;

	/**
	* Initializes a new instance of the Cluster class.
	* @param fs		file system.
	* @param path	path of the hdfs file where the nodes of the cluster are stored.
	* @param id		cluster identifier.
	* @throws IOException
	*/
	public Cluster( FileSystem fs, Path path, int id ) throws IOException
	{
		this.fs = fs;
		this.path = path;
		this.id = id;
		this.buffer = new BufferedReader( new InputStreamReader( fs.open( path ) ) );
	}
	
	/**
	 * Return the identifier value of this cluster.
	 * @return 	the identifier value.
	 */
	public int getID()
	{
		return this.id;
	}
	
	/**
	 * Restart the \see Iterator from the beginning of the cluster file.
	 * This operation is really expensive since it close the \see BufferedReader,
	 * used to scan the hdfs file, and then it opens a new one.
	 * @throws IOException
	 */
	public void restart() throws IOException
	{
		// Close previous BufferedReader
		this.buffer.close();
		
		// Open a new one
		this.buffer = new BufferedReader( new InputStreamReader( fs.open( this.path ) ) );
	}
	
	/**
	 * Check if there are still node to read.
	 * @return 	<c>true</c> if there are still nodes to read, <c>false</c> otherwise.
	 */
	public boolean hasNext()
	{
		try { return this.buffer.ready(); }
		catch (IOException e) { return false; }
	}

	/**
	 * Retrieved the next node from the hdfs file.
	 * @return 	the node read.
	 */
	public Integer next()
	{
		try { return Integer.parseInt( this.buffer.readLine() ); }
		catch (NumberFormatException e) { return null; }
		catch (IOException e) { return null; }
	}

	/** This method is not implemented by this class. */
	public void remove()
	{
		throw new UnsupportedOperationException("Remove not supported!"); 
	}
	
	/**
	 * Scanning all the hdfs file, it prints on screen all the nodes encountered.
	 * If the user wants to do something else after this operation, it has to call the restart method.
	 * @throws IOException
	 */
	public void print() throws IOException
	{
		boolean first = true;
		System.out.print( "Cluster[" + this.getID() + "] = { ");
		
		while ( this.hasNext() )
		{
			Integer node = this.next();
			if ( node == null )
				break;
			if ( !first )
				System.out.print( ',' );
			System.out.print( node );
			first = false;
		}
		
		System.out.println( " }" );
	}
	
	/**
	 * Scanning all the hdfs file, it check if the cluster contains the <em>nodeID</em>.
	 * If the user wants to do something else after this operation, it has to call the restart method.
	 * @param nodeID	node identifier to check.
	 * @return 			<c>true</c> if there cluster contains the node, <c>false</c> otherwise.
	 * @throws IOException
	 */
	public boolean contains ( int nodeID ) throws IOException
	{
		while ( this.hasNext() )
		{
			Integer node = this.next();
			if ( node == null )
				break;
			
			if ( nodeID == node )
			{
				this.restart();
				return true;
			}
		}
		return false;
	}
	
	/**
	 * When the object is called from the GC, it close the \see BufferedReader used.
	 * @throws IOException
	 */
	protected void finalize() throws IOException
	{
		// Close the BufferedReader
		this.buffer.close();
	}
}
