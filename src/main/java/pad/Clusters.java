/**
 *	@file Clusters.java
 *	@brief An array of \see Cluster objects.
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
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * An array of \see Cluster objects.
 * The need for this class is due to the fact that we need to clean the hdfs folder where the
 * clusters file are stored.
 * So we need to expose to the user a method that allow him to delete that folder.
 */
public class Clusters implements Iterable<Cluster>
{
	/** size of the array */
	public final int length;
	
	private FileSystem fs;
	private Path inputDir;
	private Cluster clusters[];

	/**
	* Initializes a new instance of the Clusters class.
	* It expects to find exactly <em>numClusters</em> cluster files into the <em>inputDir</em>.
	* @param fs				file system.
	* @param inputDir		path of the hdfs folder where clusters file are stored.
	* @param numClusters	number of clusters.
	* @throws IOException
	*/
	public Clusters( FileSystem fs, Path inputDir, int numClusters ) throws IOException
	{
		this.fs = fs;
		this.length = numClusters;
		this.inputDir = inputDir;
		this.clusters = new Cluster[numClusters];
		for ( int i = 0; i < numClusters; i++ )
			this.clusters[i] = new Cluster( fs, inputDir.suffix( "/cluster_" + i ), i );
	}
	
	/**
	* Retrieve the \see Cluster in the <em>index</em>-th position of the array. 
	* @param index	index of the \see Cluster element we want to retrieve.
	* @return		the \see Cluster element.
	* @throws IndexOutOfBoundsException if the <em>index</em> is not valid.
	*/
	public Cluster get( int index )
	{
		if ( index < 0 || index >= this.length )
			throw new IndexOutOfBoundsException( "Index: " + index + ", Length " + this.length );
		
		return this.clusters[index];
	}
	
	/**
	 * This method has to be called when the user has finish to use the clusters.
	 * It deletes the folder where the clusters file where stored.
	 * In this way the hdfs is left clean.
	 * @throws IOException
	 */
	public void destroy() throws IOException
	{
		this.fs.delete( this.inputDir, true );
	}

	/**
	 * Return an iterator on the array of clusters
	 * return	an iterator on the array of clusters.
	 */
	public Iterator<Cluster> iterator()
	{
		return Arrays.asList( this.clusters ).iterator();
	}
}
