package org.neo4j.graphalgo.core.sources;

import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipWeights;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.PropertyItem;

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

                final Cursor<NodeItem> nodeItemCursor;
                if (labelId == ReadOperations.ANY_LABEL) {
                    nodeItemCursor = readOp.nodeCursorGetAll();
                } else {
                    nodeItemCursor = readOp.nodeCursorGetForLabel(labelId);
                }

                nodeItemCursor.forAll(node -> {
                    node.relationships(Direction.BOTH, relationId).forAll(relationshipItem -> {
                        final Cursor<PropertyItem> propertyItemCursor = relationshipItem.property(propertyId);
                        if (propertyItemCursor.next()) {
                            final PropertyItem propertyItem = propertyItemCursor.get();
                            final long startNode = relationshipItem.startNode();
                            builder.withWeight(
                                    idMapping.toMappedNodeId(startNode),
                                    idMapping.toMappedNodeId(relationshipItem.otherNode(startNode)),
                                    RawValues.extractValue(propertyItem.value(), propertyDefaultValue));
                        }
                    });
                });
            });

            return builder.build();
        }
    }
}
