/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
