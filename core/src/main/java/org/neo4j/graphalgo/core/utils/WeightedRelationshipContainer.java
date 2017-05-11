package org.neo4j.graphalgo.core.utils;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.*;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;

import java.util.Iterator;

/**
 * Preallocated Container for a single relationship type including weights.
 *
 * The weights are scaled up to integer-values using {@link WeightedRelationshipContainer#multiplicand} and
 * saved next to the targetNodeId in the relationship-int-array
 *
 * @author mknblch
 */
@Deprecated
public class WeightedRelationshipContainer {

    private static final Iterator<WeightedRelationshipCursor> EMPTY_IT = new Iterator<WeightedRelationshipCursor>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public WeightedRelationshipCursor next() {
            return null;
        }
    };

    public static final int DEFAULT_MULTIPLICAND = 100_000_000;

    private final int[][] data;

    public final int multiplicand;

    private WeightedRelationshipContainer(int nodeCount, int multiplicand) {
        data = new int[nodeCount][];
        this.multiplicand = multiplicand;
    }

    /**
     * called for each relationship for nodeId
     * @param nodeId the source nodeid
     * @param consumer the relation consumer
     */
    public void forEach(int nodeId, WeightedRelationshipConsumer consumer) {
        if (nodeId >= data.length) {
            return;
        }
        final int[] relationships = data[nodeId];
        for (int i = 0; i < relationships.length; i += 2) {
            consumer.accept(nodeId, relationships[i], -1L, (double) relationships[i + 1] / multiplicand);
        }
    }

    /**
     * get an iterator for each relation
     */
    public Iterator<WeightedRelationshipCursor> iterator(int nodeId) {
        if (nodeId >= data.length) {
            return EMPTY_IT;
        }
        return new WeightedRelationIterator(data, nodeId, multiplicand);
    }

    /**
     * get RelationContainer.Builder to ease construction of the container
     *
     * @param nodeCount the nodeCount
     * @param multiplicand scales double weights to int (the higher the value the higher precision)
     * @return a Builder for RelationContainers
     */
    public static WeightedBuilder builder(int nodeCount, int multiplicand) {
        return new WeightedBuilder(nodeCount, multiplicand);
    }

    public static WeightedBuilder builder(int nodeCount) {
        return builder(nodeCount, DEFAULT_MULTIPLICAND);
    }


    /**
     * get an Importer object to ease the import from neo4j
     * @param api the core api
     * @return an Importer for RelationContainer
     */
    public static WRCImporter importer(GraphDatabaseAPI api) {
        return new WRCImporter(api);
    }

    private static class WeightedRelationIterator implements Iterator<WeightedRelationshipCursor> {

        private final WeightedRelationshipCursor cursor;
        private final int[] relationships;
        private final int size;
        private final int multiplicand;
        private int offset = 0;

        private WeightedRelationIterator(int[][] data, int sourceNodeId, int multiplicand) {
            this.multiplicand = multiplicand;
            this.relationships = data[sourceNodeId];
            size = relationships.length;
            cursor = new WeightedRelationshipCursor();
            cursor.sourceNodeId = sourceNodeId;
        }

        @Override
        public boolean hasNext() {
            return offset < size;
        }

        @Override
        public WeightedRelationshipCursor next() {
            cursor.targetNodeId = relationships[offset];
            cursor.weight = (double) relationships[offset + 1] / multiplicand;
            offset += 2;
            return cursor;
        }
    }

    public static class WeightedBuilder {

        private final WeightedRelationshipContainer container;
        private final int multiplicand;

        private int nodeId;
        private int offset;

        private WeightedBuilder(int nodeCount, int multiplicand) {
            this.multiplicand = multiplicand;
            container = new WeightedRelationshipContainer(nodeCount, multiplicand);
        }

        /**
         * set the builder to aim for nodeId and initialize its underlying connection array
         *
         * @param nodeId the current (mapped) nodeId
         * @param degree the degree for the node
         * @return this instance for method chaining
         */
        public WeightedBuilder aim(int nodeId, int degree) {
            this.nodeId = nodeId;
            offset = 0;
            container.data[nodeId] = new int[degree * 2];
            return this;
        }

        /**
         * Adds a new connection from the node which the Builder is pointing to.
         * @param targetNodeId the targetNodeId
         * @param weight the weight of the edge
         * @return itself for method chaining
         */
        public WeightedBuilder add(int targetNodeId, double weight) {
            container.data[nodeId][offset] = targetNodeId;
            container.data[nodeId][offset + 1] = (int) (weight * multiplicand);
            offset += 2;
            return this;
        }

        /**
         * builds the container
         */
        public WeightedRelationshipContainer build() {
            return container;
        }
    }

    public static class WRCImporter extends Importer<WeightedRelationshipContainer, WRCImporter> {

        private IdMapping idMapping;
        private Direction direction;

        private WRCImporter(GraphDatabaseAPI api) {
            super(api);
        }

        /**
         * set the idmapping function to use
         */
        public WRCImporter withIdMapping(IdMapping idMapping) {
            this.idMapping = idMapping;
            return this;
        }

        /**
         * set the direction to use
         */
        public WRCImporter withDirection(org.neo4j.graphdb.Direction direction) {
            this.direction = Directions.mediate(direction);
            return this;
        }

        /**
         * build the container
         */
        @Override
        protected WeightedRelationshipContainer buildT() {
            if (null == idMapping) {
                throw new IllegalArgumentException("No IdMapping given");
            }
            if (null == direction) {
                throw new IllegalArgumentException("No Direction given");
            }
            final WeightedBuilder builder = WeightedRelationshipContainer.builder(nodeCount);
            withinTransaction(read -> {
                read.nodeCursorGetAll().forAll(nodeItem -> {
                    importNode(builder, nodeItem);
                });
            });
            return builder.build();
        }

        private void importNode(WeightedBuilder builder, NodeItem nodeItem) {

            final long neo4jId = nodeItem.id();
            final int nodeId = idMapping.toMappedNodeId(neo4jId);
            if (null == relationId) {
                builder.aim(nodeId, nodeItem.degree(direction));
            } else {
                builder.aim(nodeId, nodeItem.degree(direction, relationId[0]));
            }
            nodeItem.relationships(direction, relationId).forAll(ri -> {
                double weight = propertyDefaultValue;
                if (propertyId != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                    final Cursor<PropertyItem> property = ri.property(propertyId);
                    if (property.next()) {
                        final PropertyItem propertyItem = property.get();
                        weight = extractValue(propertyItem.value());
                    }
                }

                builder.add(idMapping.toMappedNodeId(ri.otherNode(neo4jId)), weight);
            });
        }

        private double extractValue(Object value) {
            if (value instanceof Number) {
                Number number = (Number) value;
                return number.doubleValue();
            }
            if (value instanceof String) {
                String s = (String) value;
                if (!s.isEmpty()) {
                    return Double.parseDouble(s);
                }
            }
            if (value instanceof Boolean) {
                if ((Boolean) value) {
                    return 1d;
                }
            }
            // TODO: arrays

            return propertyDefaultValue;
        }

        @Override
        protected WRCImporter me() {
            return this;
        }
    }
}
