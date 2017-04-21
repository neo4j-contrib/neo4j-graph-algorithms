package org.neo4j.graphalgo.core.sources;

import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Iterator;

/**
 * Unbuffered RelationshipIterator working directly with Neo4j
 *
 * @author mknblch
 */
public class SingleRunAllRelationIterator implements AllRelationshipIterator {

    private final GraphDatabaseAPI api;
    private final IdMapping idMapping;

    public SingleRunAllRelationIterator(GraphDatabaseAPI api, IdMapping idMapping) {
        this.api = api;
        this.idMapping = idMapping;
    }

    @Override
    public void forEachRelationship(RelationshipConsumer consumer) {
        try (Transaction transaction = api.beginTx();
             Statement statement = api.getDependencyResolver()
                    .resolveDependency(ThreadToStatementContextBridge.class)
                    .get()) {
            final ReadOperations readOperations = statement
                    .readOperations();
            readOperations.relationshipCursorGetAll().forAll(c -> {
                long startNode = c.startNode();
                consumer.accept(
                        idMapping.toMappedNodeId(startNode),
                        idMapping.toMappedNodeId(c.otherNode(startNode)),
                        -1L);
            });
            transaction.success();
        }
    }

    @Override
    public Iterator<RelationshipCursor> allRelationshipIterator() {
        throw new UnsupportedOperationException();
    }
}
