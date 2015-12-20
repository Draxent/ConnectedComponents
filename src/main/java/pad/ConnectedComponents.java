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

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import pad.StarDriver.StarDriverType;

/**	This class orchestrates all the driver jobs in order to get a file with the recognized clusters of the input graph. */
public class ConnectedComponents
{
	private final String input;
	private final Path baseInput;
	private final FileSystem fs;
	
	/**
	* Initializes a new instance of the ConnectedComponents class.
	* @param graphInput		path of the input graph stored on hdfs.
	*/
	public ConnectedComponents( String graphInput ) throws IOException
	{		
		this.input = graphInput;
		this.baseInput =  new Path( FilenameUtils.removeExtension( graphInput ) );
		this.fs = FileSystem.get( new Configuration() );
	}
	
	/**
	* Delete the output folder in use by the Job that cause the error and return <c>false</c>.
	* @param suffix		in order to identify the folder to delete.
	* @return			always <c>false</c>.
	*/
	private boolean exit( String suffix ) throws IllegalArgumentException, IOException
	{
		this.fs.delete( this.baseInput.suffix( suffix ), true  );
		return false;
	}
	
	/**
	 * Execute all the Driver Job orchestration necessary to construct the array of \see Cluster.
	 * The pseudo code is the following:
	 * <code>
	 *	Initialization_Driver()
	 *
	 *	repeat
	 * 	|	Large-Star_Driver()
	 * 	|	Small-Star_Driver()
	 *  until Convercence()
	 *  
	 *	Termination_Driver()
	 * </code>
	 * @return 	<c>false</c> if the orchestration failed, <c>true</c> otherwise. 
	 * @throws Exception
	 */
	public boolean run() throws Exception
	{	
		// Run initialization in order to transform the adjacent list or cluster list into
		// a list of pair <nodeID, neighborID>.
		InitializationDriver init = new InitializationDriver( this.input, false );
		if ( init.run( null ) != 0 )
			return exit( "0" );
		
		StarDriver largeStar, smallStar;
		int i = 0;
		do
		{
			largeStar = new StarDriver( StarDriverType.LARGE, this.baseInput, i, false );
			if ( largeStar.run( null ) != 0 )
				return exit( "_" + i );
			
			// Delete previous output
			this.fs.delete( this.baseInput.suffix( "_" + i ), true  );
			i++;
			
			smallStar = new StarDriver( StarDriverType.SMALL, this.baseInput, i, false );
			if ( smallStar.run( null ) != 0 )
				return exit( "_" + i );
			
			// Delete previous output
			this.fs.delete( this.baseInput.suffix( "_" + i ), true  );
			i++;
		}
		while ( largeStar.getNumChanges() + smallStar.getNumChanges() != 0 );
		
		// Run it in order to transform the list of pair <nodeID, neighborID> into sets of nodes (clusters)
		TerminationDriver term = new TerminationDriver( this.baseInput.suffix( "_" + i ), this.baseInput.suffix( "_out" ), false );
		if ( term.run( null ) != 0 )
			return exit( "_" + i );

		// Delete last iteration
		this.fs.delete(  this.baseInput.suffix( "_" + i ), true  );
			
		return true;
	}
}
