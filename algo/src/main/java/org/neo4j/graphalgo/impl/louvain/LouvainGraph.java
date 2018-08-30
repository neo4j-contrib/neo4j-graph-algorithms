package org.neo4j.graphalgo.impl.louvain;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.IntPredicate;

/**
 * virtual graph used by Louvain
 *
 * @author mknblch
 */
public class LouvainGraph implements Graph {

    private final int nodeCount;
    private final IntObjectMap<? extends IntContainer> graph;
    private final LongDoubleMap weights;

    LouvainGraph(int newNodeCount, IntObjectMap<? extends IntContainer> graph, LongDoubleMap weights) {
        this.nodeCount = newNodeCount;
        this.graph = graph;
        this.weights = weights;
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        // not implemented
        return -1;
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        // not implemented
        return -1L;
    }

    @Override
    public boolean contains(long nodeId) {
        // not implemented
        return false;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer) {
        final IntContainer intCursors = graph.get(nodeId);
        if (null == intCursors) {
            return;
        }
        intCursors.forEach((IntProcedure) t -> consumer.accept(nodeId, t, -1));
    }

    @Override
    public double weightOf(int sourceNodeId, int targetNodeId) {
        return weights.getOrDefault(RawValues.combineIntInt(sourceNodeId, targetNodeId), 0);
    }

    @Override
    public String getType() {
        // not implemented
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void canRelease(boolean canRelease) {
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        return ParallelUtil.batchIterables(batchSize, nodeCount);
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        final IntContainer intContainer = graph.get(nodeId);
        if (null == intContainer) {
            return 0;
        }
        return intContainer.size();
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public boolean exists(int sourceNodeId, int targetNodeId, Direction direction) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        throw new IllegalStateException("not implemented");
    }

    @Override
    public int getTarget(int nodeId, int index, Direction direction) {
        return -1;
    }
}
