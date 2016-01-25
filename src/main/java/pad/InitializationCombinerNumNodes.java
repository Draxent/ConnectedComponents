/**
 *	@file InitializationCombinerNumNodes.java
 *	@brief Combiner task of the \see InitializationDriver Job.
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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**	Combiner task of the \see InitializationDriver Job. */
public class InitializationCombinerNumNodes extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
{
	public static final IntWritable MINUS_ONE = new IntWritable( -1 );
	
	/**
	* Reduce method of the this InitializationCombinerNumNodes class.
	* Reduce the number of nodes duplicated.
	* @param nodeID		node identifier.
	* @param _			not used.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void reduce( IntWritable nodeID, Iterable<IntWritable> _, Context context ) throws IOException, InterruptedException 
	{
		// Write the node identifier only one time, so eliminating many duplicates.
		context.write ( nodeID, MINUS_ONE );
	}
}