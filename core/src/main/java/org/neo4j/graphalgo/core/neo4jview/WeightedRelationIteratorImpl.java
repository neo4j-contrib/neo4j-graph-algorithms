package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightedRelationCursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.io.Closeable;
import java.util.Iterator;

class WeightedRelationIteratorImpl implements Iterator<WeightedRelationCursor>, Closeable {

    private final Graph graph;
    private final Statement statement;
    private final Transaction tx;
    private final int propertyKey;
    private final WeightedRelationCursor cursor;
    private final long originalNodeId;
    private final Cursor<RelationshipItem> relationships;
    private double defaultWeight;

    WeightedRelationIteratorImpl(Graph graph, GraphDatabaseAPI api, int sourceNodeId, Direction direction, int relationId, int propertyId, double propertyDefaultWeight) throws EntityNotFoundException {
        this.graph = graph;
        this.propertyKey = propertyId;
        this.defaultWeight = propertyDefaultWeight;
        this.tx = api.beginTx();
        this.statement = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class)
                .get();
        this.originalNodeId = graph.toOriginalNodeId(sourceNodeId);
        this.cursor = new WeightedRelationCursor();
        cursor.sourceNodeId = sourceNodeId;
        final Cursor<NodeItem> nodeCursor = statement.readOperations().nodeCursor(originalNodeId);
        nodeCursor.next();
        this.relationships = nodeCursor.get().relationships(mediate(direction), relationId);
    }

    @Override
    public boolean hasNext() {
        final boolean hasNext = relationships.next();
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    @Override
    public WeightedRelationCursor next() {

        final RelationshipItem relationshipItem = relationships.get();
        cursor.targetNodeId = graph.toMappedNodeId(relationshipItem.otherNode(originalNodeId));
        final Cursor<PropertyItem> propertyCursor = relationshipItem.property(propertyKey);
        if(!propertyCursor.next()) {
            cursor.weight = defaultWeight;
            return cursor;
        }

        final PropertyItem propertyItem = propertyCursor.get();
        cursor.weight = (double) propertyItem.value();
        return cursor;
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


    @Override
    public void close() {
        statement.close();
        tx.success();
        tx.close();
    }
}