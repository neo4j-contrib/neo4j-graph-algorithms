package org.neo4j.graphalgo.core.utils.container;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.Directions;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;

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

    /**
     * called for each relationship for nodeId
     * @param nodeId the source nodeid
     * @param consumer the relation consumer
     */
    public void forEach(int nodeId, RelationshipConsumer consumer) {
        if (nodeId >= data.length) {
            return;
        }
        final int[] relationships = data[nodeId];
        for (int relationship : relationships) {
            consumer.accept(nodeId, relationship, -1L);
        }
    }

    /**
     * get an iterator for each relation
     */
    public Iterator<RelationshipCursor> iterator(int nodeId) {
        if (nodeId >= data.length) {
            return EMPTY_IT;
        }
        return new RelationIterator(data, nodeId);
    }

    /**
     * get RelationContainer.Builder to ease construction of the container
     * @param nodeCount the nodeCount
     * @return a Builder for RelationContainers
     */
    public static Builder builder(int nodeCount) {
        return new Builder(nodeCount);
    }

    /**
     * get an Importer object to ease the import from neo4j
     * @param api the core api
     * @return an Importer for RelationContainer
     */
    public static RCImporter importer(GraphDatabaseAPI api) {
        return new RCImporter(api);
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

    public static class Builder {

        private final RelationshipContainer container;

        private int nodeId;
        private int offset;

        private Builder(int nodeCount) {
            container = new RelationshipContainer(nodeCount);
        }

        /**
         * set the builder to aim for nodeId and initialize its underlying connection array
         *
         * @param nodeId the current (mapped) nodeId
         * @param degree the degree for the node
         * @return this instance for method chaining
         */
        public Builder aim(int nodeId, int degree) {
            this.nodeId = nodeId;
            offset = 0;
            container.data[nodeId] = new int[degree];
            return this;
        }

        /**
         * Adds a new connection from the node which the Builder is pointing to.
         * @param targetNodeId the targetNodeId
         * @return itself for method chaining
         */
        public Builder add(int targetNodeId) {
            container.data[nodeId][offset++] = targetNodeId;
            return this;
        }

        /**
         * builds the container
         */
        public RelationshipContainer build() {
            return container;
        }
    }

    public static class RCImporter extends Importer<RelationshipContainer, RCImporter> {

        private IdMapping idMapping;
        private Direction direction;

        private RCImporter(GraphDatabaseAPI api) {
            super(api);
        }

        /**
         * set the idmapping function to use
         */
        public RCImporter withIdMapping(IdMapping idMapping) {
            this.idMapping = idMapping;
            return this;
        }

        /**
         * set the direction to use
         */
        public RCImporter withDirection(org.neo4j.graphdb.Direction direction) {
            this.direction = Directions.mediate(direction);
            return this;
        }

        /**
         * build the container
         */
        @Override
        protected RelationshipContainer buildT() {
            if (null == idMapping) {
                throw new IllegalArgumentException("No IdMapping given");
            }
            if (null == direction) {
                throw new IllegalArgumentException("No Direction given");
            }
            final Builder builder = RelationshipContainer.builder(nodeCount);
            forEachNodeItem(nodeItem -> importNode(builder, nodeItem));
            return builder.build();
        }

        private void importNode(Builder builder, NodeItem nodeItem) {
            final long neo4jId = nodeItem.id();
            final int nodeId = idMapping.toMappedNodeId(neo4jId);
            if (null == relationId) {
                builder.aim(nodeId, nodeItem.degree(direction));
            } else {
                builder.aim(nodeId, nodeItem.degree(direction, relationId[0]));
            }
            nodeItem.relationships(direction, relationId).forAll(ri -> {
                builder.add(idMapping.toMappedNodeId(ri.otherNode(neo4jId)));
            });
        }

        @Override
        protected RCImporter me() {
            return this;
        }
    }
}
