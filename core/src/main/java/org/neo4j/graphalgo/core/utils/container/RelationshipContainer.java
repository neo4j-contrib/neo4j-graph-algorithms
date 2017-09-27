package org.neo4j.graphalgo.core.utils.container;

import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 * Preallocated Container for a single relationship type.
 *
 * @author mknblch
 */
public class RelationshipContainer {

    private final int[][] data;

    private RelationshipContainer(int nodeCount) {
        data = new int[nodeCount][];
    }

    /**
     * called for each relationship for nodeId
     *
     * @param nodeId   the source nodeid
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
     * get RelationContainer.Builder to ease construction of the container
     *
     * @param nodeCount the nodeCount
     * @return a Builder for RelationContainers
     */
    public static Builder builder(int nodeCount) {
        return new Builder(nodeCount);
    }

    /**
     * get an Importer object to ease the import from neo4j
     *
     * @param api the core api
     * @return an Importer for RelationContainer
     */
    public static RCImporter importer(GraphDatabaseAPI api) {
        return new RCImporter(api);
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
         *
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
        public RCImporter withDirection(Direction direction) {
            this.direction = direction;
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
            final RelVisitor visitor = new RelVisitor(builder, idMapping);
            forEachNodeItem((read, nodeId) -> {
                try {
                    importNode(builder, nodeId, read, visitor);
                } catch (EntityNotFoundException e) {
                    throw Exceptions.launderedException(e);
                }
            });
            return builder.build();
        }

        private void importNode(Builder builder, long neo4jId, ReadOperations read, RelVisitor visitor)
        throws EntityNotFoundException {
            final int nodeId = idMapping.toMappedNodeId(neo4jId);
            final RelationshipIterator rels;
            if (null == relationId) {
                builder.aim(nodeId, read.nodeGetDegree(neo4jId, direction));
                rels = read.nodeGetRelationships(neo4jId, direction);
            } else {
                builder.aim(nodeId, read.nodeGetDegree(neo4jId, direction, relationId[0]));
                rels = read.nodeGetRelationships(neo4jId, direction, relationId);
            }

            visitor.neoId = neo4jId;
            while (rels.hasNext()) {
                final long relId = rels.next();
                rels.relationshipVisit(relId, visitor);
            }
        }

        @Override
        protected RCImporter me() {
            return this;
        }
    }

    private static final class RelVisitor implements RelationshipVisitor<RuntimeException> {
        private final Builder builder;
        private final IdMapping idMapping;

        private long neoId;

        private RelVisitor(Builder builder, IdMapping idMapping) {
            this.builder = builder;
            this.idMapping = idMapping;
        }

        @Override
        public void visit(long relationshipId, int typeId, long startNodeId, long endNodeId) throws RuntimeException {
            long otherId = startNodeId == neoId ? endNodeId : startNodeId;
            builder.add(idMapping.toMappedNodeId(otherId));
        }
    }
}
