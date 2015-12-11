/**
 *	@file TerminationTest.java
 *	@brief Test the \see TerminationDriver Job.
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

package test;

import java.io.IOException;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import pad.CheckDriver;
import pad.TerminationDriver;

/**	Test the \see TerminationDriver Job. */
public class TerminationTest
{
	static FileSystem fs;	
	static String graphInput;
	static String output;
	
	public static void exit( ) throws IllegalArgumentException, IOException
	{
		fs.delete( new Path( output ), true  );
		System.exit( 1 );
	}
	
	public static void main( String[] args ) throws Exception 
	{
		if ( args.length != 2 )
		{
			System.out.println( "Usage: TerminationTest <graph_input> <output>" );
			System.exit(1);
		}
		
		fs = FileSystem.get( new Configuration() );
		graphInput = FilenameUtils.removeExtension( args[0] );
		output = args[1];
		String expected_input = graphInput + "_0";
		
		// Rename input in order to match the StarDriver expected input
		fs.rename( new Path( args[0] ), new Path( expected_input ) );
		
		System.out.println( "Start TerminationDriver. " );
		TerminationDriver term = new TerminationDriver( expected_input, output, true );
		if ( term.run( null) != 0 ) exit();
		System.out.println( "End TerminationDriver." );
		
		// Rename input back, in order to leave hdfs consistent
		fs.rename( new Path( expected_input ), new Path( args[0] ) );
		
		System.out.println( "Start CheckDriver. " );
		CheckDriver check = new CheckDriver( output, true );
		if ( check.run( null ) != 0 )
			System.exit( 1 );
		System.out.println( "End CheckDriver." );

		System.exit( 0 );
	}
}