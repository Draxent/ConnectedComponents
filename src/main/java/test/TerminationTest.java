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

import pad.TerminationDriver;
import test.TranslatorDriver.DirectionTranslation;

/**	Test the \see TerminationDriver Job. */
public class TerminationTest
{
	static FileSystem fs;
	
	public static void exit( Path p ) throws IllegalArgumentException, IOException
	{
		fs.delete( p, true  );
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
		Path input = new Path( args[0] );
		Path baseInput = new Path( FilenameUtils.removeExtension( args[0] ) );
		Path output = new Path ( args[1] );
		
		// Convert text input into pair of nodes
		System.out.println( "Start TranslatorDriver Text2Pair. " );
		TranslatorDriver trans = new TranslatorDriver( input, DirectionTranslation.Text2Pair );
		if ( trans.run( null ) != 0 )
			exit( input.suffix( "_transl" ) );
		System.out.println( "End TranslatorDriver Text2Pair." );
		
		// Rename input in order to match the TerminationDriver expected input
		fs.delete( input, true  );
		fs.rename( input.suffix( "_transl" ), baseInput.suffix( "_0" ) );
		
		System.out.println( "Start TerminationDriver. " );
		TerminationDriver term = new TerminationDriver( baseInput.suffix( "_0" ), output, true );
		if ( term.run( null ) != 0 )
			exit( baseInput.suffix( "_0" ) );
		System.out.println( "End TerminationDriver." );
		
		// Convert cluster into text.
		System.out.println( "Start TranslatorDriver Cluster2Text. " );
		TranslatorDriver trans2 = new TranslatorDriver( output, DirectionTranslation.Cluster2Text );
		if ( trans2.run( null ) != 0 )
			exit( output.suffix( "_transl" ) );
		System.out.println( "End TranslatorDriver Cluster2Text." );
		
		// Rename input back, in order to leave hdfs consistent
		fs.rename( baseInput.suffix( "_0" ), input );
		
		// Delete previous output and rename result
		fs.delete( output, true  );
		fs.rename( output.suffix( "_transl" ), output );

		System.exit( 0 );
	}
}