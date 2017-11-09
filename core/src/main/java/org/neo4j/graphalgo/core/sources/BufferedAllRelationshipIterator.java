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

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.graphalgo.api.AllRelationshipIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

/**
 * Buffered AllRelationshipIterator based on hppc container
 *
 * @author mknblch
 */
public class BufferedAllRelationshipIterator implements AllRelationshipIterator {

    private final LongArrayList container;

    private BufferedAllRelationshipIterator(LongArrayList container) {
        this.container = container;
    }

    public void forEachRelationship(RelationshipConsumer consumer) {
        container.forEach((Consumer<LongCursor>) longCursor ->
                consumer.accept(
                        RawValues.getHead(longCursor.value),
                        RawValues.getTail(longCursor.value),
                        -1L));
    }

    public static BufferedAllRelationshipIterator.Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final LongArrayList container;

        public Builder() {
            container = new LongArrayList();
        }

        /**
         * add connection between source and target
         */
        public Builder add(int sourceNodeId, int targetNodeId) {
            container.add(RawValues.combineIntInt(sourceNodeId, targetNodeId));
            return this;
        }

        public BufferedAllRelationshipIterator build() {
            return new BufferedAllRelationshipIterator(container);
        }
    }

    public static BARIImporter importer(GraphDatabaseAPI api) {
        return new BARIImporter(api);
    }

    public static class BARIImporter extends Importer<BufferedAllRelationshipIterator, BARIImporter> {

        private IdMapping idMapping;

        public BARIImporter(GraphDatabaseAPI api) {
            super(api);
        }

        public BARIImporter withIdMapping(IdMapping idMapping) {
            this.idMapping = idMapping;
            return this;
        }

        @Override
        protected BARIImporter me() {
            return this;
        }

        @Override
        protected BufferedAllRelationshipIterator buildT() {
            final Builder builder = BufferedAllRelationshipIterator.builder();
            withinTransaction(readOp ->
                    SingleRunAllRelationIterator.forAll(readOp, (relationshipId, typeId, startNodeId, endNodeId) ->
                            builder.add(idMapping.toMappedNodeId(startNodeId), idMapping.toMappedNodeId(endNodeId))));
            return builder.build();
        }
    }
}
