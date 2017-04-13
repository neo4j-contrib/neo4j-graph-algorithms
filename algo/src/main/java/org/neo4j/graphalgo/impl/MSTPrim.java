package org.neo4j.graphalgo.impl;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.LongDoubleCursor;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.queue.LongMinPriorityQueue;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;

import static org.neo4j.graphalgo.core.utils.RawValues.*;

/**
 * Impl. Prim's Minimum Weight Spanning Tree
 *
 * @author mknobloch
 */
public class MSTPrim {


    private final IdMapping idMapping;
    private final WeightedRelationshipIterator iterator;

    public MSTPrim(IdMapping idMapping, WeightedRelationshipIterator iterator) {
        this.idMapping = idMapping;
        this.iterator = iterator;
    }

    /**
     * compute the minimum weight spanning tree starting at node startNode
     *
     * @param startNode the node to start the evaluation from
     * @return a container of the transitions in the minimum spanning tree
     */
    public MinimumSpanningTree compute(int startNode) {

        final LongMinPriorityQueue queue = new LongMinPriorityQueue();
        final MinimumSpanningTree mst = new MinimumSpanningTree();
        final BitSet visited = new BitSet(idMapping.nodeCount());

        // initially add all relations from startNode to the priority queue
        visited.set(startNode);
        iterator.forEachRelationship(startNode, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
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
            iterator.forEachRelationship(nodeId, Direction.BOTH, (sourceNodeId, targetNodeId, relationId, weight) -> {
                queue.add(combineIntInt(nodeId, targetNodeId), weight);
            });
        }

        return mst;
    }

    public static class MinimumSpanningTree implements WeightedAllRelationshipIterator {

        private final LongDoubleMap costMap;

        public MinimumSpanningTree() {
            this(8);
        }

        public MinimumSpanningTree(int expectedElements) {
            costMap = new LongDoubleScatterMap(expectedElements);
        }

        public void add(int sourceNodeId, int targetNodeId, double cost) {
            final long combined = RawValues.combineIntInt(sourceNodeId, targetNodeId);
            costMap.put(combined, cost);
        }

        @Override
        public void forEachRelationship(WeightedRelationshipConsumer consumer) {
            for (LongDoubleCursor cursor : costMap) {
                consumer.accept(
                        RawValues.getHead(cursor.key),
                        RawValues.getTail(cursor.key),
                        -1L,
                        costMap.get(cursor.key));
            }
        }

        @Override
        public Iterator<WeightedRelationshipCursor> allWeightedRelationshipIterator() {
            return new AllIterator(costMap);
        }

        private static class AllIterator implements Iterator<WeightedRelationshipCursor> {

            private final Iterator<LongDoubleCursor> iterator;
            private final LongDoubleMap costMap;
            private final WeightedRelationshipCursor cursor = new WeightedRelationshipCursor();

            private AllIterator(LongDoubleMap costMap) {
                this.iterator = costMap.iterator();
                this.costMap = costMap;
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public WeightedRelationshipCursor next() {
                final LongDoubleCursor next = iterator.next();
                cursor.sourceNodeId = RawValues.getHead(next.key);
                cursor.targetNodeId = RawValues.getTail(next.key);
                cursor.weight = next.value;
                return cursor;
            }
        }

    }
}
