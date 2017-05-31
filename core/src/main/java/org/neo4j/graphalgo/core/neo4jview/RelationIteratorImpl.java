package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.RawValues;
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
import java.util.Iterator;

class RelationIteratorImpl implements Iterator<RelationshipCursor>, Closeable {

    private final Graph graph;
    private final Transaction transaction;
    private final Statement statement;
    private final RelationshipIterator iterator;
    private final ReadOperations read;
    private final RelationshipCursor cursor;
    private final long originalNodeId;
    private final IdCombiner relId;

    RelationIteratorImpl(Graph graph, GraphDatabaseAPI api, int sourceNodeId, Direction direction, int relationTypeId) throws EntityNotFoundException {
        this.graph = graph;
        transaction = api.beginTx();
        statement = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class)
                .get();
        read = statement.readOperations();
        originalNodeId = graph.toOriginalNodeId(sourceNodeId);
        if (relationTypeId == ReadOperations.ANY_RELATIONSHIP_TYPE) {
            iterator = read.nodeGetRelationships(originalNodeId, direction);
        } else {
            iterator = read.nodeGetRelationships(originalNodeId, direction, relationTypeId);
        }
        cursor = new RelationshipCursor();
        cursor.sourceNodeId = sourceNodeId;
        relId = RawValues.combiner(direction);
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
    public RelationshipCursor next() {
        final long relationId = iterator.next();
        final Cursor<RelationshipItem> relCursor = read.relationshipCursor(relationId);
        relCursor.next();
        final RelationshipItem item = relCursor.get();
        cursor.targetNodeId = graph.toMappedNodeId(item.otherNode(originalNodeId));
        cursor.relationshipId = relId.apply(cursor);
        return cursor;
    }

    @Override
    public void close() {
        statement.close();
        transaction.success();
        transaction.close();
    }
}
