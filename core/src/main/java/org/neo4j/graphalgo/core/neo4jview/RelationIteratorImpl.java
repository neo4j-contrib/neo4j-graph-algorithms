package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationCursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.RelationshipItem;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

class RelationIteratorImpl implements Iterator<RelationCursor>, Closeable {

    private final Graph graph;
    private final GraphDatabaseAPI api;
    private final Transaction transaction;
    private final Statement statement;
    private final RelationshipIterator iterator;
    private final ReadOperations read;
    private final RelationCursor cursor;
    private final long originalNodeId;

    RelationIteratorImpl(Graph graph, GraphDatabaseAPI api, int sourceNodeId, Direction direction, int relationTypeId) throws EntityNotFoundException {
        this.graph = graph;
        this.api = api;
        transaction = api.beginTx();
        statement = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class)
                .get();
        read = statement.readOperations();
        originalNodeId = graph.toOriginalNodeId(sourceNodeId);
        iterator = read.nodeGetRelationships(originalNodeId, direction, relationTypeId);
        cursor = new RelationCursor();
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
    public RelationCursor next() {
        final long relationId = iterator.next();
        final Cursor<RelationshipItem> relCursor = read.relationshipCursor(relationId);
        relCursor.next();
        final RelationshipItem item = relCursor.get();
        cursor.relationId = relationId;
        cursor.targetNodeId = graph.toMappedNodeId(item.otherNode(originalNodeId));
        return cursor;
    }

    @Override
    public void close() {
        statement.close();
        transaction.success();
        transaction.close();
    }
}