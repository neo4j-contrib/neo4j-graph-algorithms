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

import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * @author mknblch
 */
public class BufferedWeightMap implements RelationshipWeights {

    private final double propertyDefaultWeight;
    private final LongDoubleMap data;

    private BufferedWeightMap(LongDoubleMap data, double propertyDefaultWeight) {
        this.data = data;
        this.propertyDefaultWeight = propertyDefaultWeight;
    }

    @Override
    public double weightOf(int sourceNodeId, int targetNodeId) {
        return data.getOrDefault(RawValues.combineIntInt(sourceNodeId, targetNodeId), propertyDefaultWeight);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static WeightImporter importer(GraphDatabaseAPI api) {
        return new WeightImporter(api);
    }

    public static final class Builder {

        private final LongDoubleMap map;

        private double propertyDefaultWeight = 0.0d;
        private boolean anyDirection = false;

        public Builder() {
            map = new LongDoubleScatterMap();
        }

        public Builder withAnyDirection(boolean anyDirection) {
            this.anyDirection = anyDirection;
            return this;
        }

        public Builder withWeight(int sourceNodeId, int targetNodeId, double weight) {
            if (weight == propertyDefaultWeight) {
                return this;
            }
            map.put(RawValues.combineIntInt(sourceNodeId, targetNodeId), weight);
            if (anyDirection) {
                map.put(RawValues.combineIntInt(targetNodeId, sourceNodeId), weight);
            }
            return this;
        }

        public BufferedWeightMap build() {
            return new BufferedWeightMap(map, propertyDefaultWeight);
        }
    }

    public static class WeightImporter extends Importer<BufferedWeightMap, WeightImporter> {

        private IdMapping idMapping;
        private boolean anyDirection = false;

        public WeightImporter(GraphDatabaseAPI api) {
            super(api);
        }

        public WeightImporter withIdMapping(IdMapping idMapping) {
            this.idMapping = idMapping;
            return this;
        }

        public WeightImporter withAnyDirection(boolean anyDirection) {
            this.anyDirection = anyDirection;
            return this;
        }

        @Override
        protected WeightImporter me() {
            return this;
        }

        @Override
        protected BufferedWeightMap buildT() {

            final Builder builder = BufferedWeightMap.builder()
                    .withAnyDirection(anyDirection);

            withinTransaction(readOp -> {
                final RelationshipVisitor<RuntimeException> visitor;
                if (relationId == null) {
                    visitor = (relationshipId, typeId, startNodeId, endNodeId) -> {
                        try {
                            final Object value = readOp.relationshipGetProperty(
                                    relationshipId,
                                    propertyId);
                            if (value != null) {
                                builder.withWeight(
                                        idMapping.toMappedNodeId(startNodeId),
                                        idMapping.toMappedNodeId(endNodeId),
                                        RawValues.extractValue(value, propertyDefaultValue));
                            }
                        } catch (EntityNotFoundException ignored) {
                        }
                    };
                } else {
                    final int targetType = relationId[0];
                    visitor = (relationshipId, typeId, startNodeId, endNodeId) -> {
                        if (typeId == targetType) {
                            try {
                                final Object value = readOp.relationshipGetProperty(
                                        relationshipId,
                                        propertyId);
                                if (value != null) {
                                    builder.withWeight(
                                            idMapping.toMappedNodeId(startNodeId),
                                            idMapping.toMappedNodeId(endNodeId),
                                            RawValues.extractValue(value, propertyDefaultValue));
                                }
                            } catch (EntityNotFoundException ignored) {
                            }
                        }

                    };
                }

                if (labelId == ReadOperations.ANY_LABEL) {
                    readAllWeights(readOp, visitor);
                } else {
                    readLabelWeights(readOp, visitor, relationId);
                }
            });

            return builder.build();
        }

        private void readAllWeights(
                ReadOperations readOp,
                RelationshipVisitor<RuntimeException> visitor) {
            SingleRunAllRelationIterator.forAll(readOp, visitor);
        }

        private void readLabelWeights(
                ReadOperations readOp,
                RelationshipVisitor<RuntimeException> visitor,
                int[] relationId) {
            try {
                final PrimitiveLongIterator nodes = readOp.nodesGetForLabel(labelId);
                while (nodes.hasNext()) {
                    final long nodeId = nodes.next();
                    final RelationshipIterator rels;
                    if (relationId != null) {
                        rels = readOp.nodeGetRelationships(
                                nodeId,
                                org.neo4j.graphdb.Direction.BOTH,
                                relationId);
                    } else {
                        rels = readOp.nodeGetRelationships(nodeId, org.neo4j.graphdb.Direction.BOTH);
                    }
                    while (rels.hasNext()) {
                        final long relId = rels.next();
                        rels.relationshipVisit(relId, visitor);
                    }
                }
            } catch (EntityNotFoundException e) {
                throw Exceptions.launderedException(e);
            }
        }
    }
}
