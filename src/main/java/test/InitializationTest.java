/**
 *	@file InitializationTest.java
 *	@brief Test the \see InitializationDriver Job.
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

import pad.InitializationDriver;

/**	Test the \see InitializationDriver Job. */
public class InitializationTest
{
	static FileSystem fs;
	static String graphInput;
	
	public static void exit() throws IllegalArgumentException, IOException
	{
		fs.delete( new Path( graphInput + "_0" ), true  );
		System.exit( 1 );
	}
	
	public static void main( String[] args ) throws Exception 
	{
		if ( args.length != 1 )
		{
			System.out.println( "Usage: InitializationTest <graph_input>" );
			System.exit(1);
		}
		fs = FileSystem.get( new Configuration() );
		graphInput = FilenameUtils.removeExtension( args[0] );
		
		System.out.println( "Start InitializationDriver. " );
		InitializationDriver init = new InitializationDriver( args[0], true );
		if ( init.run( null ) != 0 ) exit();
		System.out.println( "End InitializationDriver." );

		System.exit( 0 );
	}
}