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
package org.neo4j.graphalgo.impl.yens;

import org.neo4j.graphalgo.similarity.SimilarityResult;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.StatementApi;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Values;

import java.util.stream.Stream;

public class SimilarityExporter extends StatementApi {

    private final int propertyId;
    private final int relationshipTypeId;

    public SimilarityExporter(GraphDatabaseAPI api,
                              String relationshipType,
                              String propertyName) {
        super(api);
        propertyId = getOrCreatePropertyId(propertyName);
        relationshipTypeId = getOrCreateRelationshipId(relationshipType);
    }

    public void export(Stream<SimilarityResult> similarityPairs) {
        writeSequential(similarityPairs);
    }

    private void export(SimilarityResult similarityResult) {
        applyInTransaction(statement -> {
            long node1 = similarityResult.item1;
            long node2 = similarityResult.item2;

            try {
                long relationshipId = statement.dataWrite().relationshipCreate(node1, relationshipTypeId, node2);

                statement.dataWrite().relationshipSetProperty(
                        relationshipId, propertyId, Values.doubleValue(similarityResult.similarity));
            } catch (KernelException e) {
                ExceptionUtil.throwKernelException(e);
            }
            return null;
        });

    }

    private int getOrCreateRelationshipId(String relationshipType) {
        return applyInTransaction(stmt -> stmt
                .tokenWrite()
                .relationshipTypeGetOrCreateForName(relationshipType));
    }

    private int getOrCreatePropertyId(String propertyName) {
        return applyInTransaction(stmt -> stmt
                .tokenWrite()
                .propertyKeyGetOrCreateForName(propertyName));
    }

    private void writeSequential(Stream<SimilarityResult> similarityPairs) {
        similarityPairs.forEach(this::export);
    }


}
