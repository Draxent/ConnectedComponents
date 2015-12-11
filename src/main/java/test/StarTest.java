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

/**	Test the \see StarDriver Job. */
public class StarTest
{
	static FileSystem fs;
	static String graphInput;
	
	public static void exit() throws IllegalArgumentException, IOException
	{
		fs.delete( new Path( graphInput + "_1" ), true  );
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
		graphInput = FilenameUtils.removeExtension( args[1] );
		
		// Rename input in order to match the StarDriver expected input
		fs.rename( new Path( args[1] ), new Path( graphInput + "_0" ) );
		
		StarDriverType type = args[0].toLowerCase().equals("small") ? StarDriverType.SMALL : StarDriverType.LARGE;
		String name = ( type == StarDriverType.SMALL ) ? "Small" : "Large";
		
		System.out.println( "Start " + name + "-Star." );
		StarDriver largeStar = new StarDriver( type, graphInput, 0, true );
		if ( largeStar.run( null ) != 0 ) exit();
		System.out.println( "End " + name + "-Star.");
		
		// Rename input back, in order to leave hdfs consistent
		fs.rename( new Path( graphInput + "_0" ), new Path( args[1] ) );
		
		System.exit( 0 );
	}
}