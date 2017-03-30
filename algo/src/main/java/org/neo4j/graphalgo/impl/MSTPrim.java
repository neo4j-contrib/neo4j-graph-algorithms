package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.LongMinPriorityQueue;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import static org.neo4j.graphalgo.core.utils.RawValues.*;

/**
 * work in progress
 *
 * This implementation returns a Collection of long values where each long encodes
 * a transition from a sourceNodeId (carried in the 4 most significant bytes)
 * and its targetNodeId (in the 4 least significant bytes).
 *
 * The HelperClass {@link RawValues} can be used to encode and decode the values.
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
     * @return an encoded container of the transitions in the minimum spanning tree
     */
    public LongArrayList compute(int startNode) { // TODO replace OUTGOING with BOTH

        final LongMinPriorityQueue queue = new LongMinPriorityQueue();
        final LongArrayList mst = new LongArrayList(graph.nodeCount()); // TODO better container
        final IntSet visited = new IntHashSet(); // TODO same

        // initially add all relations from startNode
        graph.forEachRelation(startNode, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
            queue.add(combineIntInt(startNode, targetNodeId), weight);
        });

        while (!queue.isEmpty()) {
            final long transition = queue.pop();
            final int nodeId = getTail(transition);
            if (visited.contains(nodeId)) {
                continue;
            }
            visited.add(nodeId);
            mst.add(transition);
            graph.forEachRelation(nodeId, Direction.OUTGOING, (sourceNodeId, targetNodeId, relationId, weight) -> {
                queue.add(combineIntInt(nodeId, targetNodeId), weight);
            });
        }

        return mst;
    }

}
