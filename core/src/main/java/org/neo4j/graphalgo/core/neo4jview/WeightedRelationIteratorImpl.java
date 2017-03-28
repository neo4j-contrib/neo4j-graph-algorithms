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
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

import java.io.Closeable;
import java.util.Iterator;

class WeightedRelationIteratorImpl implements Iterator<WeightedRelationCursor>, Closeable {

    private final Graph graph;
    private final GraphDatabaseAPI api;
    private final Statement statement;
    private final Transaction tx;
    private final int propertyKey;
    private final RelationshipIterator iterator;
    private final ReadOperations read;
    private final WeightedRelationCursor cursor;
    private final long originalNodeId;
    private double defaultWeight;

    WeightedRelationIteratorImpl(Graph graph, GraphDatabaseAPI api, Transaction tx, int sourceNodeId, Direction direction, int propertyKey, double propertyDefaultWeight) throws EntityNotFoundException {
        this.graph = graph;
        this.api = api;
        statement = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class)
                .get();
        this.tx = tx;
        this.propertyKey = propertyKey;
        defaultWeight = propertyDefaultWeight;
        read = statement.readOperations();
        originalNodeId = graph.toOriginalNodeId(sourceNodeId);
        iterator = read.nodeGetRelationships(originalNodeId, direction);
        cursor = new WeightedRelationCursor();
        cursor.sourceNodeId = sourceNodeId;
    }

    @Override
    public boolean hasNext() {
        final boolean hasNext = iterator.hasNext();
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    @Override
    public WeightedRelationCursor next() {
        final long relationId = iterator.next();
        try(Cursor<RelationshipItem> relCursor = read.relationshipCursor(relationId)) {
            relCursor.next();
            final RelationshipItem item = relCursor.get();
            cursor.targetNodeId = graph.toMappedNodeId(item.otherNode(originalNodeId));
            cursor.relationId = relationId;
            try(final Cursor<PropertyItem> itemCursor = item.property(propertyKey)) {
                if (itemCursor.next()) {
                    final PropertyItem propertyItem = itemCursor.get();
                    cursor.weight = (double) propertyItem.value();
                } else{
                    cursor.weight = defaultWeight;
                }
            }
        }
        return cursor;
    }

    @Override
    public void close() {
        statement.close();
        tx.success();
        tx.close();
    }
}