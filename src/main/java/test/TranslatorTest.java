/**
 *	@file TranslatorTest.java
 *	@brief Test the \see TranslatorDriver Job.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import test.TranslatorDriver.DirectionTranslation;

/**	Test the \see TranslatorDriver Job. */
public class TranslatorTest
{
	static FileSystem fs;
	static Path input;
	
	public static void exit( String suffix ) throws IllegalArgumentException, IOException
	{
		fs.delete( input.suffix( suffix ), true  );
		System.exit( 1 );
	}
	
	public static void main( String[] args ) throws Exception 
	{
		if ( args.length != 2 )
		{
			System.out.println( "Usage: TranslatorTest <graph_input> <direction>" );
			System.exit(1);
		}
		
		fs = FileSystem.get( new Configuration() );
		input = new Path( args[0] );
		
		DirectionTranslation direction;
		args[1] = args[1].toLowerCase();
		
		if ( args[1].equals( "pair2text" ) ) direction = DirectionTranslation.Pair2Text;
		else if ( args[1].equals( "text2pair" ) ) direction = DirectionTranslation.Text2Pair;
		else direction = DirectionTranslation.Cluster2Text;
		
		System.out.println( "Start TranslatorDriver " + direction.toString() + "." );
		TranslatorDriver trans = new TranslatorDriver( input, direction );
		if ( trans.run( null ) != 0 )
			exit( "_transl" );
		System.out.println( "End TranslatorDriver " + direction.toString() + "." );

		System.exit( 0 );
	}
}