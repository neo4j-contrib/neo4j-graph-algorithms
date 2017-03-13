package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.Iterator;
import java.util.function.IntConsumer;

/**
 * @author mknobloch
 */
public class GraphView implements WeightedGraph, AutoCloseable {

    private final Transaction transaction;
    private final Statement statement;
    private final ReadOperations read;
    private final int vertexCount;

    private final int propertyKey;

    public GraphView(GraphDatabaseAPI db, String propertyName) {
        transaction = db.beginTx();
        statement = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class)
                .get();
        read = statement.readOperations();
        vertexCount = Math.toIntExact(read.nodesGetCount());
        propertyKey = read.propertyKeyGetForName(propertyName);
    }

    @Override
    public void forEachEdge(int vertexId, Direction direction, EdgeConsumer consumer) {
        try (Cursor<NodeItem> nodeItemCursor = read.nodeCursor(toNodeId(vertexId))) {
            while (nodeItemCursor.next()) {
                try (Cursor<RelationshipItem> relationships = nodeItemCursor
                        .get()
                        .relationships(direction == Direction.INCOMING ? org.neo4j.storageengine.api.Direction.INCOMING : org.neo4j.storageengine.api.Direction.OUTGOING)) {

                    while (relationships.next()) {
                        final RelationshipItem item = relationships.get();

                        final long edgeId = item.id();
                        consumer.accept(
                                vertexId,
                                toVertexId(item.otherNode(vertexId)),
                                edgeId
                        );
                    }
                }

            }
        }
    }

    @Override
    public void forEachEdge(int vertexId, Direction direction, WeightedEdgeConsumer consumer) {
        try {
            final RelationshipIterator iterator = read.nodeGetRelationships(vertexId, direction);
            while (iterator.hasNext()) {
                final long edgeId = iterator.next();
                final Cursor<RelationshipItem> relationshipItemCursor = read.relationshipCursor(edgeId);
                relationshipItemCursor.next();
                final RelationshipItem item = relationshipItemCursor.get();
                consumer.accept(
                        vertexId,
                        toVertexId(item.otherNode(vertexId)),
                        edgeId,
                        (double) read.relationshipGetProperty(edgeId, propertyKey)
                );
            }
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<WeightedEdgeCursor> weightedEdgeIterator(int vertexId, Direction direction) {
        try {
            return new WeightedEdgeIterator(vertexId, read, read.nodeGetRelationships(toNodeId(vertexId), direction), propertyKey);
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int vertexCount() {
        return vertexCount;
    }

    @Override
    public void forEachVertex(IntConsumer consumer) {
        try (Cursor<NodeItem> nodeItemCursor = read.nodeCursorGetAll()) {
            while (nodeItemCursor.next()) {
                final NodeItem nodeItem = nodeItemCursor.get();
                consumer.accept(toVertexId(nodeItem.id()));
            }
        }
    }

    @Override
    public PrimitiveIntIterator vertexIterator() {
        final PrimitiveLongIterator iterator = read.nodesGetAll();

        return new PrimitiveIntIterator() { // TODO static class
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public int next() {
                return toVertexId(iterator.next());
            }
        };
    }

    @Override
    public int degree(int vertexId, Direction direction) {
        try {
            return read.nodeGetDegree(toNodeId(vertexId), direction);
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<EdgeCursor> edgeIterator(int vertexId, Direction direction) {
        try {
            final RelationshipIterator iterator = read.nodeGetRelationships(toNodeId(vertexId), direction);
            return new EdgeIterator(vertexId, read, iterator);
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int toVertexId(long nodeId) {
        return Math.toIntExact(nodeId);
    }

    @Override
    public long toNodeId(int vertexId) {
        return vertexId;
    }

    @Override
    public void close() throws Exception {
        statement.close();
        transaction.success();
        transaction.close();
    }

}
