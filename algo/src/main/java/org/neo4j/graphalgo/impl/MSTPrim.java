package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.heavyweight.mst.Edge;
import org.neo4j.graphalgo.core.heavyweight.mst.EdgeQueue;
import org.neo4j.graphalgo.core.utils.IntMinPriorityQueue;
import org.neo4j.graphalgo.core.heavyweight.mst.MinimumSpanningTree;
import org.neo4j.graphdb.Direction;

/**
 * @author mknobloch
 */
public class MSTPrim {

    private final Graph graph;

    public MSTPrim(Graph graph) {
        this.graph = graph;
    }

    public MinimumSpanningTree compute(int startNode) {

        final MinimumSpanningTree mst = new MinimumSpanningTree();
        final EdgeQueue queue = new EdgeQueue(graph.nodeCount());

        graph.forEachRelation(startNode, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
            queue.add(sourceNodeId, targetNodeId, weight);
        });

        while (!queue.isEmpty()) {
            final Edge edge = queue.poll();
            if (mst.containsTarget(edge.targetNode)) {
                continue;
            }
            mst.add(edge);
            graph.forEachRelation(edge.targetNode, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
                queue.add(sourceNodeId, targetNodeId, weight);
            });
        }

        return mst;
    }
}
