/**
 *	@file ConnectedComponents.java
 *	@brief This class orchestrates all the driver jobs in order to get a file with the recognized clusters of the input graph.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import pad.InitializationDriver.InputType;
import pad.StarDriver.StarDriverType;

/**	This class orchestrates all the driver jobs in order to get a file with the recognized clusters of the input graph. */
public class ConnectedComponents
{
	private static final int MAX_ITERATIONS = 30;
	private final Path input, output;
	private final FileSystem fs;
	private InputType type;
	private long numCliques, numInitialNodes, numNodes, numClusters;
	private boolean testOk;
	
	/**
	* Initializes a new instance of the ConnectedComponents class.
	* @param input		path of the input graph stored on hdfs.
	* @param output		path of the output folder.
	*/
	public ConnectedComponents( Path input, Path output ) throws IOException
	{		
		this.input =  input;
		this.output =  output;
		this.fs = FileSystem.get( new Configuration() );
	}
	
	/**
	 * Execute all the Driver Job orchestration necessary to construct the array of \see Cluster.
	 * The pseudo code is the following:
	 * <code>
	 *	InitializationDriver()
	 *
	 *	repeat
	 * 	|	Large-StarDriver()
	 * 	|	Small-StarDriver()
	 *  until Convercence()
	 *  
	 *	TerminationDriver()
	 *	CheckDriver()
	 * </code>
	 * @return 	<c>false</c> if the orchestration failed, <c>true</c> otherwise. 
	 * @throws Exception
	 */
	public boolean run() throws Exception
	{	
		// Run initialization in order to transform the adjacency list or cliques list into a edges list <nodeID, neighborID>.
		InitializationDriver init = new InitializationDriver( this.input, this.input.suffix( "_0" ), false );
		if ( init.run( null ) != 0 )
		{
			this.fs.delete( this.input.suffix( "_0" ), true );
			return false;
		}
		
		StarDriver largeStar, smallStar;
		int i = 0;
		do
		{
			largeStar = new StarDriver( StarDriverType.LARGE, this.input.suffix( "_" + i ), this.input.suffix( "_" + (i+1) ), i, false );
			if ( largeStar.run( null ) != 0 )
			{
				this.fs.delete( this.input.suffix( "_" + i ), true );
				this.fs.delete( this.input.suffix( "_" + (i+1) ), true );
				return false;
			}
			
			// Delete previous output
			this.fs.delete( this.input.suffix( "_" + i ), true );
			i++;
			
			smallStar = new StarDriver( StarDriverType.SMALL, this.input.suffix( "_" + i ), this.input.suffix( "_" + (i+1) ), i, false );
			if ( smallStar.run( null ) != 0 )
			{
				this.fs.delete( this.input.suffix( "_" + i ), true );
				this.fs.delete( this.input.suffix( "_" + (i+1) ), true );
				return false;
			}
			
			// Delete previous output
			this.fs.delete( this.input.suffix( "_" + i ), true );
			i++;
		}
		while ( (largeStar.getNumChanges() + smallStar.getNumChanges() != 0) && (i < 2*MAX_ITERATIONS) );
		
		// Run it in order to transform the edges list <nodeID, neighborID> into sets of nodes (clusters)
		TerminationDriver term = new TerminationDriver( this.input.suffix( "_" + i ), this.output, false );
		if ( term.run( null ) != 0 )
		{
			this.fs.delete( this.input.suffix( "_" + i ), true );
			this.fs.delete( this.output, true );
			return false;
		}

		// Delete last iteration
		this.fs.delete(  this.input.suffix( "_" + i ), true );
		
		CheckDriver check = new CheckDriver( this.output, false );
		if ( check.run( null ) != 0)
			return false;
		
		this.type = init.getInputType();
		this.numCliques = init.getNumCliques();
		this.numInitialNodes = init.getNumInitialNodes();
		this.numClusters = term.getNumClusters();
		this.numNodes = term.getNumNodes();
		this.testOk = check.isTestOk();
		
		return true;
	}
	
	/**
	 * Return the type of format of the input file.
	 * @return 	the type of format of the input file.
	 */
	public InputType getInputType()
	{
		return this.type;
	}
	
	/**
	 * Returns the number of cliques founds in the input file.
	 * @return 	number of cliques.
	 */
	public long getNumCliques()
	{
		return this.numCliques;
	}
	
	/**
	 * Returns the number of initial nodes founds in the input file.
	 * @return 	number of initial nodes.
	 */
	public long getNumInitialNodes()
	{
		return this.numInitialNodes;
	}
	
	/**
	 * Return the number of nodes found.
	 * @return 	number of nodes.
	 */
	public long getNumNodes()
	{
		return this.numNodes;
	}
	
	/**
	 * Return the number of clusters found.
	 * @return 	number of clusters.
	 */
	public long getNumClusters()
	{
		return this.numClusters;
	}
	
	/**
	 * Return <code>false</code> if the checking phase has found that at least one Cluster is malformed,
	 * <code>true</code> otherwise.
	 * @return 	<code>true</code> if no Cluster is malformed, <code>false</code> otherwise.
	 */
	public boolean isTestOk()
	{
		return this.testOk;
	}
	
	/**
	 * Main of the \see ConnectedComponents class.
	 * @param args	array of external arguments,
	 * @throws Exception
	 */
	public static void main( String[] args ) throws Exception 
	{
		if ( args.length != 2 )
		{
			System.out.println( "Usage: ConnectedComponents <input> <output>" );
			System.exit(1);
		}
		
		Path input = new Path( args[0] );
		Path output = new Path( args[1] );
		System.out.println( "Start ConnectedComponents." );
		ConnectedComponents cc = new ConnectedComponents( input, output );
		if ( !cc.run() )
			System.exit( 1 );
		System.out.println( "End ConnectedComponents." );
		
		System.out.println( "Input file format: \033[1;94m" + cc.getInputType().toString() + "\033[0m." );
		System.out.println( "Number of initial nodes: \033[1;94m" + cc.getNumInitialNodes() + "\033[0m." );
		System.out.println( "Number of Cliques: \033[1;94m" + cc.getNumCliques() + "\033[0m." );
		System.out.println( "Number of final nodes: \033[1;94m" + cc.getNumNodes() + "\033[0m." );
		System.out.println( "Number of Clusters: \033[1;94m" + cc.getNumClusters() + "\033[0m." );
		System.out.println( "TestOK: \033[1;94m" + String.valueOf( cc.isTestOk() ) + "\033[0m." );
		
		System.exit( 0 );
	}
}
