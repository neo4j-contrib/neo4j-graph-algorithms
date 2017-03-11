package org.neo4j.graphalgo.core.heavyweight;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMapping;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphdb.Direction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.IntConsumer;

/**
 * Heavy weighted graph built of an adjacency matrix.
 *
 * Pro:
 *  - ~3 times faster due to little overhead in index lookups
 *  - connections can be added in arbitrary order
 *  
 * Cons:
 *  - has a higher memory consumption // TODO evaluate
 *  - grows it's matrix dynamically // TODO replace array growing if degree is known
 * @author mknblch
 */
public class HeavyGraph implements WeightedGraph {

    private final IdMap nodeMap;
    private final AdjacencyMatrix container;
    private final WeightMapping weights;

    public HeavyGraph(
            IdMap nodeMap,
            AdjacencyMatrix container,
            final WeightMapping weights) {
        this.nodeMap = nodeMap;
        this.container = container;
        this.weights = weights;
    }

    @Override
    public int vertexCount() {
        return nodeMap.size();
    }

    @Override
    public void forEachVertex(IntConsumer consumer) {
        nodeMap.forEach(consumer);
    }

    @Override
    public PrimitiveIntIterator vertexIterator() {
        return nodeMap.iterator();
    }

    @Override
    public int degree(int vertexId, Direction direction) {
        return container.degree(vertexId, direction);
    }

    @Override
    public void forEachEdge(int vertexId, Direction direction, EdgeConsumer consumer) {
        container.forEach(vertexId, direction, consumer);
    }

    @Override
    public void forEachEdge(
            final int vertexId,
            final Direction direction,
            final WeightedEdgeConsumer consumer) {
        container.forEach(vertexId, direction, weights, consumer);
    }

    @Override
    public Iterator<EdgeCursor> edgeIterator(int vertexId, Direction direction) {
        return container.edgeIterator(vertexId, direction);
    }

    @Override
    public Iterator<WeightedEdgeCursor> weightedEdgeIterator(
            final int vertexId, final Direction direction) {
        return container.weightedEdgeIterator(vertexId, weights, direction);
    }

    @Override
    public int toVertexId(long nodeId) {
        return nodeMap.get(nodeId);
    }

    @Override
    public long toNodeId(int vertexId) {
        return nodeMap.unmap(vertexId);
    }

    public void serialize(Path outFile) throws IOException {
        HeavyGraphFileWriter.write(
                outFile,
                container.outOffsets,
                container.inOffsets,
                container.outgoing,
                container.incoming,
                container.outgoingIds,
                container.incomingIds,
                nodeMap.size(),
                nodeMap.mappedIds(),
                weights.capacity(),
                weights.weights()
        );
    }
}
