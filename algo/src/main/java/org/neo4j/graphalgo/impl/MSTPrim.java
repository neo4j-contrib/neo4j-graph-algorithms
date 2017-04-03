package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IntBinaryConsumer;
import org.neo4j.graphalgo.core.utils.LongMinPriorityQueue;
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
    public MST compute(int startNode) {

        final LongMinPriorityQueue queue = new LongMinPriorityQueue();
        final MST mst = new MST(graph.nodeCount());
        final BitSet visited = new BitSet(graph.nodeCount());

        // initially add all relations from startNode to the priority queue
        visited.set(startNode);
        graph.forEachRelation(startNode, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
            queue.add(combineIntInt(startNode, targetNodeId), weight);
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
            graph.forEachRelation(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
                queue.add(combineIntInt(nodeId, targetNodeId), weight);
            });
        }

        return mst;
    }


    public static class MST {

        private final long[] data;
        private int offset = 0;

        public MST(int capacity) {
            data = new long[capacity];
        }

        /**
         * get the element count in this mst
         */
        public int size() {
            return offset;
        }

        public void forEach(IntBinaryConsumer consumer) { // TODO RelationConsumer
            for (int i = 0; i < offset; i++) {
                final long value = data[i];
                consumer.accept(getHead(value), getTail(value));
            }
        }

        void add(long p) {
            data[offset++] = p;
        }
    }

}
