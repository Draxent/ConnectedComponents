# ConnectedComponents

###Description
Given a graph, this algorithm identifies the [connected components](https://en.wikipedia.org/wiki/Connected_component_(graph_theory)) using Hadoop MapReduce framework.

In the following, a **connected component** will be called as **"cluster"**.

The algorithm tries to implement the *"The Alternating Algorithm"* proposed in the paper [Connected Components in MapReduce and Beyond](http://dl.acm.org/citation.cfm?id=2670997). Below, I have reported the pseudocode of the algorithm.

```
	Input: Edges (u, v) as a set of key-value pairs <u; v>.
	Input: A unique label lv for every node v ∈ V .
1:	repeat
2:		Large-Star
3:		Small-Star
4:	until Convergence
```

###Demonstration
Below, it is shown a demostration of usage of the **ConnectedComponents** class that allows you to run the connected compontent algorithm on your graph.<br />
This code simply creates a **ConnectedComponents** object, with *"graph.txt"* file as input, and calls its method **run** that produces the expected result in a folder named *"graph_out"*. This folder contains an **hdfs file** for each Reducer task invoked by MapReduce framework. Each of these files contains some of the clusters found in the input graph.

```Java
package test;

import pad.ConnectedComponents;

public class App
{
	public static void main( String[] args ) throws Exception 
	{	
		// Create the ConnectedComponents object giving to it as input an adjacency list
		// or cluster list rappresenting the graph stored in a file on hdfs
		ConnectedComponents cc = new ConnectedComponents( "graph.txt" );

		// Run all the Jobs necessary to compute the result 
		if ( !cc.run() )
			System.exit( 1 );

		System.exit( 0 );
	}
}
```

In this toy application, we don't work on the computed result. But you can invoke the **TranslatorTest** as follwing:
```bash
$HADOOP_HOME/bin/hadoop jar $WORKING_DIR/$JAR_PATH test.TranslatorTest graph_out Cluster2Text
```
in order to translate the result files into text format and look with your eyes by which nodes the connected component is made up.

Otherwise, of course, you can code a MapReduce Job to perform the operation that you are looking for taking as input the *"graph_out"* folder.

###Compile
After you have cloned the project, to compile the program you'll need to use the following command lines:

```bash
cd ConnectedComponents
mvn package
```

###Usage
To run the program, you'll need to use the following command line:

```bash
$HADOOP_HOME/bin/hadoop jar target/connectedComponents-1.0-SNAPSHOT.jar test.App
```

where $HADOOP_HOME indicates the path to the root directory of hadoop.

### Input
In the [data](./data) folder, there are some graph examples that you can use to try this software. In that folder, there are a lot of files.<br />
You have to look up only to the one named as *input_${number}.txt*.<br />

The input file can contain:

1.	the **adjacency list** of the graph, i.e. multiple lines in the following format:
	```bash
	<NodeID><TAB><Neighborhood>
	```
	- `<NodeID>`: is a unique integer ID corresponding to an unique node of the graph.
	- `<Neighborhood>`: is a comma separated list of increasing unique IDs corresponding to the nodes of the graph that are linked to `<NodeID>`.

2.	the **cluster list** of the graph, i.e. multiple lines in the following format:
	```bash
	<NodeID1><SPACE><NodeID2><SPACE> ... <SPACE><NodeIDN>
	```
	indicating that al the nodes, `<NodeID1>` ... `<NodeIDN>` , are linked to each other.

### Output
The program will create a folder, named *${input_name}_out*, where the clusters found are stored.<br />
In particular the output files produced by the Reducer tasks are formatted by the `SequenceFileOutputFormat<pad.ClusterWritable, org.apache.hadoop.io.NullWritable>`, where `pad.ClusterWritable` rappresents an array of integers and is serialized writing on the output file its size and then its elements.

### Algorithm
The algorithm, that I have implemented, has some slight difference compared to the one proposed in the paper. Below, it is shown the pseudocode.

```
	Input: G = (V, E) rappresented with an adjacency list.
	Input: A unique and positive label lv for every node v ∈ V .
1:	Initialization_Phase
2:	repeat
3:		Large-Star
4:		Small-Star
5:	until Convergence
6:	Termination_Phase
```

Where:

- **Initialization_Phase**	→	Transform the adjacency/cluster list in a list of pairs `<NodeID><TAB><NeighborID>`
- **Termination_Phase**		→	Transform the list of pairs into sets of nodes ( *cluster files* )

### Test the Software
In order to test this software, I have prepared some verification outputs in the [data](./data) folder, with the purpose to compare these handmande expected outputs with the software outputs.<br />
For example, you can test the following graph:

<img src="./data/graph_1.png">

- *input_1.txt*				→	Contains the adjacency list of the graph shown in the picture.
- *init_1.txt*				→	Contains the expected result for the **Initialization_Phase** applied to *input_1.txt*.
- *large-star_1.txt*		→	Contains the expected result for the **Large-Star** operation applied to *input_1.txt*.
- *small-star_1.txt*		→	Contains the expected result for the **Small-Star** operation applied to *input_1.txt*.
- *term_1.txt*				→	It is the hand-made input for the **Termination_Phase**, in this way we can check the last phase without running all the algorithm.
- *cluster_1.txt*	→	Contains a line for each cluster found, and the set of nodes that compose each cluster are stored in ascending order. It is the final expected result, so it is the expected result for the **Termination_Phase** applied to *term_1.txt* and the expected result of the all algorithm as well.

	The content of the file, in this case, is the following:

	```
	1 18 19
	20
	2 4 8 10 12 13 15 16 17
	3 5 7 9 14
	6 11
	```

In the [bin](./bin) folder, you can find a *bash script* that tests each phase for every appropriate input found in the [data](./data) folder. Pay attenction that for the *StarTest.sh* script, you need to specify the *type* of the operation as argument, like "small" or "large".<br />
Also, verify that at least the **$HADOOP_HOME** and **$WORKING_DIR** variables are appropriately set.<br />
In my case they have the following values :

```
HADOOP_HOME=/home/$USER/hadoop-1.2.1
WORKING_DIR=/home/$USER/Exercises-PAD/connectedComponents
```

###License
Apache License

Copyright (c) 2015 Federico Conte

http://www.apache.org/licenses/LICENSE-2.0
