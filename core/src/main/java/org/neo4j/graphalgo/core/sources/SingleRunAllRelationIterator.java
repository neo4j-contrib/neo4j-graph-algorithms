package org.neo4j.graphalgo.core.sources;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

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
        final RelationshipVisitor<RuntimeException> visitor;
        visitor = (relationshipId, typeId, startNodeId, endNodeId) ->
                consumer.accept(
                        idMapping.toMappedNodeId(startNodeId),
                        idMapping.toMappedNodeId(endNodeId),
                        relationshipId);

        try (Transaction transaction = api.beginTx();
             Statement statement = api.getDependencyResolver()
                    .resolveDependency(ThreadToStatementContextBridge.class)
                    .get()) {
            final ReadOperations readOperations = statement.readOperations();
            forAll(readOperations, visitor);
            transaction.success();
        }
    }

    static void forAll(
            ReadOperations readOp,
            RelationshipVisitor<RuntimeException> visitor) {
        final PrimitiveLongIterator allRels;
        allRels = readOp.relationshipsGetAll();
        if (allRels instanceof org.neo4j.kernel.impl.api.store.RelationshipIterator) {
            org.neo4j.kernel.impl.api.store.RelationshipIterator rels = (org.neo4j.kernel.impl.api.store.RelationshipIterator) allRels;
            while (rels.hasNext()) {
                rels.relationshipVisit(rels.next(), visitor);
            }
        } else {
            try {
                while (allRels.hasNext()) {
                    final long relId = allRels.next();
                    readOp.relationshipVisit(relId, visitor);
                }
            } catch (EntityNotFoundException e) {
                throw Exceptions.launderedException(e);
            }
        }
    }
}
