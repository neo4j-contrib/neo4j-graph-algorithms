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
package org.neo4j.graphalgo.core.lightweight;

import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.WeightMap;
import org.neo4j.graphalgo.core.utils.IdCombiner;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.paged.IntArray;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

class RelationshipImporter implements RelationshipVisitor<EntityNotFoundException>, RelationshipImport {

    final ReadOperations readOp;

    private final IdMap mapping;
    private final IntArray.BulkAdder bulkAdder;
    private final long[] offsets;
    private final int[] relationId;
    private final Direction direction;

    private int sourceGraphId;

    private long adjacencyIdx;
    private int imported;

    static RelationshipImporter of(
            ReadOperations readOp,
            IdMap mapping,
            long[] offsets,
            int[] relationId,
            Direction direction,
            IntArray.BulkAdder bulkAdder,
            WeightMapping weights) {
        if (weights instanceof WeightMap) {
            return new WithWeights(
                    mapping,
                    offsets,
                    relationId,
                    readOp,
                    direction,
                    (WeightMap) weights,
                    bulkAdder);
        } else {
            return new RelationshipImporter(
                    readOp,
                    mapping,
                    offsets,
                    relationId,
                    direction,
                    bulkAdder
            );
        }
    }

    RelationshipImporter(
            ReadOperations readOp,
            IdMap mapping,
            long[] offsets,
            int[] relationId,
            Direction direction,
            IntArray.BulkAdder bulkAdder) {
        this.readOp = readOp;
        this.mapping = mapping;
        this.direction = direction;
        this.bulkAdder = bulkAdder;
        this.offsets = offsets;
        this.relationId = relationId;
    }

    public void importRelationships(int sourceGraphId, long sourceNodeId)
    throws EntityNotFoundException {
        imported = 0;
        offsets[sourceGraphId] = adjacencyIdx;
        final int degree = relationId == null
                ? readOp.nodeGetDegree(sourceNodeId, direction)
                : readOp.nodeGetDegree(sourceNodeId, direction, relationId[0]);
        if (degree > 0) {
            final RelationshipIterator rels = relationId == null
                    ? readOp.nodeGetRelationships(sourceNodeId, direction)
                    : readOp.nodeGetRelationships(sourceNodeId, direction, relationId);

            bulkAdder.init(adjacencyIdx, degree);
            this.sourceGraphId = sourceGraphId;
            while (rels.hasNext()) {
                long relId = rels.next();
                rels.relationshipVisit(relId, this);
            }
        }

        adjacencyIdx += imported;
    }

    int sourceGraphId() {
        return sourceGraphId;
    }

    long adjacencyIdx() {
        return adjacencyIdx;
    }

    @Override
    public void visit(
            final long relationshipId,
            final int typeId,
            final long startNodeId,
            final long endNodeId) throws EntityNotFoundException {
        dovisit(relationshipId, typeId, startNodeId, endNodeId);
    }

    int dovisit(
            final long relationshipId,
            final int typeId,
            final long startNodeId,
            final long endNodeId) throws EntityNotFoundException {

        long targetNodeId = direction == Direction.OUTGOING ? endNodeId : startNodeId;
        int targetGraphId = mapping.get(targetNodeId);
        if (targetGraphId == -1) {
            return -1;
        }
        bulkAdder.add(targetGraphId);
        imported++;

        return targetGraphId;
    }

    static class WithWeights extends RelationshipImporter {
        private final WeightMap weights;
        private final int weightId;
        private final IdCombiner idCombiner;

        WithWeights(
                IdMap mapping,
                long[] offsets,
                int[] relationId,
                ReadOperations readOp,
                Direction direction,
                WeightMap weights,
                IntArray.BulkAdder bulkAdder) {
            super(readOp, mapping, offsets, relationId, direction, bulkAdder);
            this.weights = weights;
            this.weightId = weights.propertyId();
            this.idCombiner = RawValues.combiner(direction);
        }

        @Override
        int dovisit(
                final long relationshipId,
                final int typeId,
                final long startNodeId,
                final long endNodeId) throws EntityNotFoundException {
            final int targetGraphId = super.dovisit(
                    relationshipId,
                    typeId,
                    startNodeId,
                    endNodeId);

            if (targetGraphId >= 0) {
                try {
                    Object value = readOp.relationshipGetProperty(relationshipId, weightId);
                    long relId = idCombiner.apply(sourceGraphId(), targetGraphId);
                    weights.set(relId, value);
                } catch (EntityNotFoundException ignored) {
                }
            }
            return targetGraphId;
        }
    }
}
