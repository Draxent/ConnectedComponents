/**
 *	@file ConnectedComponentsTest.java
 *	@brief Test the \see ConnectedComponents class.
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

import pad.ConnectedComponents;

/**	Test the \see ConnectedComponents class. */
public class ConnectedComponentsTest
{
	public static void main( String[] args ) throws Exception 
	{	
		if ( args.length != 1 )
		{
			System.out.println( "Usage: ConnectedComponentsTest <graph_input>" );
			System.exit(1);
		}
		
		System.out.println( "Start ConnectedComponents. " );
		ConnectedComponents cc = new ConnectedComponents( args[0] );
		if ( !cc.run() )
			System.exit( 1 );
		System.out.println( "End ConnectedComponents." );

		System.exit( 0 );
	}
}