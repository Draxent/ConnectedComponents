/**
 *	@file ConnectedComponents.java
 *	@brief This class orchestrates all the driver jobs in order to get an array of clusters (\see Cluster) as a result.
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

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import pad.StarDriver.StarDriverType;

/**	This class orchestrates all the driver jobs in order to get an array of clusters (\see Cluster) as a result. */
public class ConnectedComponents
{
	private final String input;
	private final String baseInput;
	private final FileSystem fs;
	private Clusters clusters;
	
	/**
	* Initializes a new instance of the ConnectedComponents class.
	* @param graphInput		path of the input graph stored on hdfs.
	*/
	public ConnectedComponents( String graphInput ) throws IOException
	{		
		this.input = graphInput;
		this.baseInput =  FilenameUtils.removeExtension( graphInput );
		this.fs = FileSystem.get( new Configuration() );
		this.clusters = null;
	}
	
	/**
	* Delete the output folder in use by the Job that cause the error and return <c>false</c>.
	* @param suffix		in order to identify the folder to delete.
	* @return			always <c>false</c>.
	*/
	private boolean exit( String suffix ) throws IllegalArgumentException, IOException
	{
		this.fs.delete( new Path( this.baseInput + "_" + suffix ), true  );
		return false;
	}
	
	/**
	 * Execute all the Driver Job orchestration necessary to construct the array of \see Cluster.
	 * The pseudo code is the following:
	 * <code>
	 *	Initialization_Driver()
	 *	for log2(N) steps do
	 * 	|	Large-Star_Driver()
	 * 	|	Small-Star_Driver()
	 *	Termination_Driver()
	 *	CheckDriver()
	 *	build the array of Cluster
	 * </code>
	 * @return 	<c>false</c> if the orchestration failed, <c>true</c> otherwise. 
	 * @throws Exception
	 */
	public boolean run() throws Exception
	{	
		// Run initialization in order to transform the adjacent list into
		// a list of pair <nodeID, neighborID> and get the number of nodes in the graph
		InitializationDriver init = new InitializationDriver( this.input, false );
		if ( init.run( null ) != 0 )
			return exit( "0" );
		
		// Cycle for log2(N) steps, where N → number of nodes 
		int logN = (int) Math.ceil( Math.log( init.getNumNodes() ) / Math.log( 2 ) ); 
		int last_iteration = 2*logN;
		for ( int i = 0; i < last_iteration; i = i+2 )
		{
			StarDriver largeStar = new StarDriver( StarDriverType.LARGE, this.baseInput, i, false );
			if ( largeStar.run( null ) != 0 )
				return exit( Integer.toString(i) );
			
			// Delete previous output
			this.fs.delete( new Path( this.baseInput + "_" + i ), true  );
			
			StarDriver smallStar = new StarDriver( StarDriverType.SMALL, this.baseInput, i + 1, false );
			if ( smallStar.run( null ) != 0 )
				return exit( Integer.toString( i + 1 ) );
			
			// Delete previous output
			this.fs.delete( new Path( this.baseInput + "_" + ( i + 1 ) ), true  );
		}
		
		// Run it in order to transform the list of pair <nodeID, neighborID> into sets of nodes (clusters)
		TerminationDriver term = new TerminationDriver( this.baseInput + "_" + last_iteration, this.baseInput + "_out", false );
		if ( term.run( null ) != 0 )
			return exit( Integer.toString( last_iteration ) );

		// Delete last iteration
		this.fs.delete( new Path( this.baseInput + "_" + last_iteration ), true  );
		
		// Run it in order to check if the clusters are well formed
		CheckDriver check = new CheckDriver( this.baseInput + "_out", false );
		if ( check.run( null ) != 0 )
			return exit( "_out" );		
		
		// Create an clusters data structure
		this.clusters = new Clusters( fs, new Path( this.baseInput + "_out" ), term.getNumClusters() );		
		return true;
	}
	
	/**
	 * Return \see Clusters data structure.
	 * @return 	\see Clusters data structure.
	 */
	public Clusters getResult()
	{
		return this.clusters;
	}
}
