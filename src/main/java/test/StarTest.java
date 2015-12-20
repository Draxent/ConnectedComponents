/**
 *	@file StartTest.java
 *	@brief Test the \see StarDriver Job.
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

import pad.StarDriver;
import pad.StarDriver.StarDriverType;
import test.TranslatorDriver.DirectionTranslation;

/**	Test the \see StarDriver Job. */
public class StarTest
{
	static FileSystem fs;
	static Path input;
	
	public static void exit( Path p ) throws IllegalArgumentException, IOException
	{
		fs.delete( p, true  );
		System.exit( 1 );
	}
	
	public static void main( String[] args ) throws Exception 
	{
		if ( args.length != 2 )
		{
			System.out.println( "Usage: StartTest <type> <graph_input>" );
			System.exit(1);
		}
		
		fs = FileSystem.get( new Configuration() );
		input = new Path( FilenameUtils.removeExtension( args[1] ) );
		Path true_input = new Path( args[1] );
		
		// Convert text input into pair of nodes
		System.out.println( "Start TranslatorDriver Text2Pair. " );
		TranslatorDriver trans = new TranslatorDriver( true_input, DirectionTranslation.Text2Pair );
		if ( trans.run( null ) != 0 )
			exit( true_input.suffix( "_transl" ) );
		System.out.println( "End TranslatorDriver Text2Pair." );
		
		// Rename input in order to match the StarDriver expected input
		fs.delete( true_input, true  );
		fs.rename( true_input.suffix( "_transl" ), input.suffix( "_0" ) );
		
		// Check what Job we need to execute: Small-Star or Large-Star
		StarDriverType type = args[0].toLowerCase().equals("small") ? StarDriverType.SMALL : StarDriverType.LARGE;
		String name = ( type == StarDriverType.SMALL ) ? "Small" : "Large";
		
		// Execute the Small-Star or Large-Star Job
		System.out.println( "Start " + name + "-Star." );
		StarDriver star = new StarDriver( type, input, 0, true );
		if ( star.run( null ) != 0 )
			exit( input.suffix( "_1" ) );
		System.out.println( "End " + name + "-Star.");
		
		// Convert pair of nodes into text.
		System.out.println( "Start TranslatorDriver Pair2Text. " );
		TranslatorDriver trans2 = new TranslatorDriver( input.suffix( "_1" ), DirectionTranslation.Pair2Text );
		if ( trans2.run( null ) != 0 )
			exit( input.suffix( "_1_transl" ) );
		System.out.println( "End TranslatorDriver Pair2Text." );
		
		// Rename input back, in order to leave hdfs consistent
		fs.rename( input.suffix( "_0" ), true_input );
		
		// Delete previous output and rename result
		fs.delete( input.suffix( "_1" ), true  );
		fs.rename( input.suffix( "_1_transl" ), input.suffix( "_1" ) );
		
		System.exit( 0 );
	}
}