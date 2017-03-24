package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.util.Iterator;
import java.util.function.IntConsumer;

/**
 * A Graph implemented as View on Neo4j Kernel API
 *
 * @author mknobloch
 */
public class GraphView implements Graph, AutoCloseable {

    private final Transaction transaction;
    private final Statement statement;
    private final ReadOperations read;

    private final int nodeCount;
    private final int propertyKey;
    private final int labelId;

    public GraphView(GraphDatabaseAPI db, String label, String propertyName) {
        transaction = db.beginTx();
        statement = db.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class)
                .get();
        read = statement.readOperations();
        labelId = read.labelGetForName(label);
        nodeCount = Math.toIntExact(read.nodesGetCount());
        propertyKey = read.propertyKeyGetForName(propertyName);
    }

/*    @Override
    public void forEachRelation(int nodeId, Direction direction, RelationConsumer consumer) {
        final long originalNodeId = toOriginalNodeId(nodeId);
        try (Cursor<NodeItem> nodeItemCursor = read.nodeCursor(originalNodeId)) {
            while (nodeItemCursor.next()) {
                final NodeItem nodeItem = nodeItemCursor.get();
                try (Cursor<RelationshipItem> relationships = nodeItem.relationships(mediate(direction))) {
                    while (relationships.next()) {
                        final RelationshipItem item = relationships.get();
                        consumer.accept(nodeId, toMappedNodeId(item.otherNode(originalNodeId)), item.id());
                    }
                }
            }
        }
    }*/

    @Override
    public void forEachRelation(int nodeId, Direction direction, RelationConsumer consumer) {
        final long originalNodeId = toOriginalNodeId(nodeId);
        try (Cursor<NodeItem> nodeItemCursor = read.nodeCursor(originalNodeId)) {

            nodeItemCursor.forAll(nodeItem -> {
                try (Cursor<RelationshipItem> relationships = nodeItem.relationships(mediate(direction))) {
                    relationships.forAll(item -> {
                        consumer.accept(nodeId, toMappedNodeId(item.otherNode(originalNodeId)), item.id());
                    });
                }
            });
        }
    }

    @Override
    public void forEachRelation(int nodeId, Direction direction, WeightedRelationConsumer consumer) {
        try {
            final RelationshipIterator iterator = read.nodeGetRelationships(nodeId, direction);
            while (iterator.hasNext()) {
                final long relationId = iterator.next();
                final Cursor<RelationshipItem> relationshipItemCursor = read.relationshipCursor(relationId);
                relationshipItemCursor.next();
                final RelationshipItem item = relationshipItemCursor.get();
                consumer.accept(
                        nodeId,
                        toMappedNodeId(item.otherNode(nodeId)),
                        relationId,
                        (double) read.relationshipGetProperty(relationId, propertyKey)
                );
            }
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<WeightedRelationCursor> weightedRelationIterator(int nodeId, Direction direction) {
        try {
            return new WeightedRelationIteratorImpl(nodeId, read, read.nodeGetRelationships(toOriginalNodeId(nodeId), direction), propertyKey);
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int nodeCount() {
        return nodeCount;
    }

    @Override
    public void forEachNode(IntConsumer consumer) {
        if (labelId == StatementConstants.NO_SUCH_LABEL) {
            try (Cursor<NodeItem> nodeItemCursor = read.nodeCursorGetAll()) {
                while (nodeItemCursor.next()) {
                    consumer.accept(toMappedNodeId(nodeItemCursor.get().id()));
                }
            }
        } else {
            try (Cursor<NodeItem> nodeItemCursor = read.nodeCursorGetForLabel(labelId)) {
                while (nodeItemCursor.next()) {
                    consumer.accept(toMappedNodeId(nodeItemCursor.get().id()));
                }
            }
        }
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        if (labelId == StatementConstants.NO_SUCH_LABEL) {
            return new NodeIterator(this, read.nodesGetAll());
        }
        return new NodeIterator(this, read.nodesGetForLabel(labelId));
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        try {
            return read.nodeGetDegree(toOriginalNodeId(nodeId), direction);
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<RelationCursor> relationIterator(int nodeId, Direction direction) {
        try {
            return new RelationIteratorImpl(nodeId, read, read.nodeGetRelationships(toOriginalNodeId(nodeId), direction));
        } catch (EntityNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        return Math.toIntExact(nodeId);
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return nodeId;
    }

    @Override
    public void close() throws Exception {
        statement.close();
        transaction.success();
        transaction.close();
    }

    private static org.neo4j.storageengine.api.Direction mediate(Direction direction) {
        switch (direction) {
            case INCOMING:
                return org.neo4j.storageengine.api.Direction.INCOMING;
            case OUTGOING:
                return org.neo4j.storageengine.api.Direction.OUTGOING;
            case BOTH:
                return org.neo4j.storageengine.api.Direction.BOTH;
        }
        throw new IllegalArgumentException("Direction " + direction + " is unknown");
    }


    private static class NodeIterator implements PrimitiveIntIterator {

        private final Graph graph;

        private final PrimitiveLongIterator iterator;

        private NodeIterator(Graph graph, PrimitiveLongIterator iterator) {
            this.graph = graph;
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public int next() {
            return graph.toMappedNodeId(iterator.next());
        }
    }
}
