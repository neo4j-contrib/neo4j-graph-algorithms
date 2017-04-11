package org.neo4j.graphalgo.core.utils;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;

import java.util.Iterator;

/**
 * Preallocated Container for a single relationship type.
 *
 * @author mknblch
 */
public class RelationshipContainer {

    private static final Iterator<RelationshipCursor> EMPTY_IT = new Iterator<RelationshipCursor>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public RelationshipCursor next() {
            return null;
        }
    };

    private final int[][] data;

    private RelationshipContainer(int nodeCount) {
        data = new int[nodeCount][];
    }

    public void forEach(int nodeId, RelationshipConsumer consumer) {
        if (nodeId >= data.length) {
            return;
        }
        final int[] relationships = data[nodeId];
        for (int relationship : relationships) {
            consumer.accept(nodeId, relationship, -1L);
        }
    }

    public Iterator<RelationshipCursor> iterator(int nodeId) {
        if (nodeId >= data.length) {
            return EMPTY_IT;
        }
        return new RelationIterator(data, nodeId);
    }

    private static class RelationIterator implements Iterator<RelationshipCursor> {

        private final RelationshipCursor cursor;
        private final int[] relationships;
        private final int size;
        private int offset = 0;

        private RelationIterator(int[][] data, int sourceNodeId) {
            this.relationships = data[sourceNodeId];
            size = relationships.length;
            cursor = new RelationshipCursor();
            cursor.sourceNodeId = sourceNodeId;
        }

        @Override
        public boolean hasNext() {
            return offset < size;
        }

        @Override
        public RelationshipCursor next() {
            cursor.targetNodeId = relationships[offset++];
            return cursor;
        }
    }

    public static Builder builder(int nodeCount) {
        return new Builder(nodeCount);
    }

    public static RCImporter importer(GraphDatabaseAPI api) {
        return new RCImporter(api);
    }

    public static class Builder {

        private final RelationshipContainer container;

        private int nodeId;
        private int offset;

        public Builder(int nodeCount) {
            container = new RelationshipContainer(nodeCount);
        }

        public Builder aim(int nodeId, int degree) {
            this.nodeId = nodeId;
            offset = 0;
            container.data[nodeId] = new int[degree];
            return this;
        }

        public Builder add(int targetNodeId) {
            container.data[nodeId][offset++] = targetNodeId;
            return this;
        }

        public RelationshipContainer build() {
            return container;
        }
    }

    public static class RCImporter extends Importer<RelationshipContainer, RCImporter> {

        private IdMapping idMapping;
        private Direction direction;

        public RCImporter(GraphDatabaseAPI api) {
            super(api);
        }

        @Override
        protected RCImporter me() {
            return this;
        }


        public RCImporter withIdMapping(IdMapping idMapping) {
            this.idMapping = idMapping;
            return this;
        }

        public RCImporter withDirection(org.neo4j.graphdb.Direction direction) {
            switch (direction) {
                case OUTGOING:
                    this.direction = Direction.OUTGOING;
                    break;
                case INCOMING:
                    this.direction = Direction.INCOMING;
                    break;
                case BOTH:
                    this.direction = Direction.BOTH;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown direction");
            }
            return this;
        }

        @Override
        public RelationshipContainer build() {

            if (null == idMapping) {
                throw new IllegalArgumentException("No IdMapping given");
            }
            if (null == direction) {
                throw new IllegalArgumentException("No Direction given");
            }

            final Builder builder = RelationshipContainer.builder(nodeCount);
            withinTransaction(read -> {
                read.nodeCursorGetAll().forAll(nodeItem -> {
                    final long neo4jId = nodeItem.id();
                    builder.aim(idMapping.toMappedNodeId(neo4jId), nodeItem.degree(direction));
                    nodeItem.relationships(direction, relationId).forAll(ri -> {
                        builder.add(idMapping.toMappedNodeId(ri.otherNode(neo4jId)));
                    });
                });

            });
            return builder.build();
        }
    }
}
