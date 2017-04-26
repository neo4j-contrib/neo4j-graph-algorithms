package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.container.SubGraph;
import org.neo4j.graphalgo.core.utils.queue.LongMinPriorityQueue;

import static org.neo4j.graphalgo.core.utils.RawValues.*;

/**
 * Impl. Prim's Minimum Weight Spanning Tree
 *
 * @author mknobloch
 */
public class MSTPrim {

    private final IdMapping idMapping;
    private final BothRelationshipIterator iterator;
    private final RelationshipWeights weights;

    public MSTPrim(IdMapping idMapping, BothRelationshipIterator iterator, RelationshipWeights weights) {
        this.idMapping = idMapping;
        this.iterator = iterator;
        this.weights = weights;
    }

    /**
     * compute the minimum weight spanning tree starting at node startNode
     *
     * @param startNode the node to start the evaluation from
     * @return a container of the transitions in the minimum spanning tree
     */
    public SubGraph compute(int startNode) {

        final LongMinPriorityQueue queue = new LongMinPriorityQueue();
        final SubGraph mst = new SubGraph(64);
        final BitSet visited = new BitSet(idMapping.nodeCount());

        // initially add all relations from startNode to the priority queue
        visited.set(startNode);
        iterator.forEachRelationship(startNode, (sourceNodeId, targetNodeId, relationId) -> {
            queue.add(combineIntInt(startNode, targetNodeId), weights.weightOf(sourceNodeId, targetNodeId));
            return true;
        });
        while (!queue.isEmpty()) {
            // retrieve cheapest transition
            final long transition = queue.pop();
            final int nodeId = getTail(transition);
            if (visited.get(nodeId)) {
                continue;
            }
            visited.set(nodeId);
            // add to mst
            mst.add(transition);
            // add new candidates
            iterator.forEachRelationship(nodeId, (sourceNodeId, targetNodeId, relationId) -> {
                queue.add(combineIntInt(nodeId, targetNodeId), weights.weightOf(sourceNodeId, targetNodeId));
                return true;
            });
        }

        return mst;
    }
}
