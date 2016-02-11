package pad;

public enum UtilCounters
{
	///  Count the number of initial nodes in the graph.
	NUM_INITIAL_NODES,
	/// Count the number of cliques in the graph (used only if the input is format as clique list).
	NUM_CLIQUES,
	/// Count the number of nodes found.
	NUM_NODES,
	/// Count the number of clusters found.
	NUM_CLUSTERS,
	/// Count the number of changes occurred during the operation Small-Star or Large-Star.
	NUM_CHANGES,
	/// Count the number of clusters found malformed from the \see CheckReducer Tasks.
	NUM_ERRORS
}
