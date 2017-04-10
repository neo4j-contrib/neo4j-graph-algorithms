package org.neo4j.graphalgo.core.sources;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Iterator;

/**
 *
 * @author mknblch
 */
public class SingleRunAllRelationIterator implements AllRelationIterator, AutoCloseable {

    private final GraphDatabaseAPI api;
    private final Transaction transaction;
    private final IdMapping idMapping;

    public SingleRunAllRelationIterator(GraphDatabaseAPI api, IdMapping idMapping) {
        this.api = api;
        transaction = api.beginTx();
        this.idMapping = idMapping;
    }

    @Override
    public void forEachRelation(RelationConsumer consumer) {
        final ReadOperations readOperations = api.getDependencyResolver()
                .resolveDependency(ThreadToStatementContextBridge.class)
                .get()
                .readOperations();
        readOperations.relationshipCursorGetAll().forAll(c -> {
            long startNode = c.startNode();
            consumer.accept(
                    idMapping.toMappedNodeId(startNode),
                    idMapping.toMappedNodeId(c.otherNode(startNode)),
                    -1L);
        });
    }

    @Override
    public Iterator<RelationCursor> allRelationIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        transaction.success();
        transaction.close();
    }
}
