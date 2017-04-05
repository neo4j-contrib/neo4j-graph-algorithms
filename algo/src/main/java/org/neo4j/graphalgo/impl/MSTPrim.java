package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IntBinaryConsumer;
import org.neo4j.graphalgo.core.utils.LongMinPriorityQueue;
import org.neo4j.graphalgo.core.utils.MinimumSpanningTree;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.RawValues.*;

/**
 * Impl. Prim's Minimum Weight Spanning Tree
 *
 * @author mknobloch
 */
public class MSTPrim {

    private final Graph graph;

    public MSTPrim(Graph graph) {
        this.graph = graph;
    }

    /**
     * compute the minimum weight spanning tree starting at node startNode
     *
     * @param startNode the node to start the evaluation from
     * @return a container of the transitions in the minimum spanning tree
     */
    public MinimumSpanningTree compute(int startNode) {

        final LongMinPriorityQueue queue = new LongMinPriorityQueue(); // TODO tuple int prio queue
        final MinimumSpanningTree mst = new MinimumSpanningTree();
        final BitSet visited = new BitSet(graph.nodeCount());

        // initially add all relations from startNode to the priority queue
        visited.set(startNode);
        graph.forEachRelation(startNode, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
            queue.add(combineIntInt(startNode, targetNodeId), weight);
        });
        while (!queue.isEmpty()) {
            // retrieve cheapest transition
            final double cost = queue.topCost();
            final long transition = queue.pop();
            final int nodeId = getTail(transition);
            if (visited.get(nodeId)) {
                continue;
            }
            visited.set(nodeId);
            // add to mst
            mst.add(getHead(transition), getTail(transition), cost);
            // add new candidates
            graph.forEachRelation(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
                if (visited.get(targetNodeId)) {
                    return;
                }
                queue.add(combineIntInt(nodeId, targetNodeId), weight);
            });
        }

        return mst;
    }
}
