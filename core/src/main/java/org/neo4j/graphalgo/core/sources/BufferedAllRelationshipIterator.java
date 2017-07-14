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
            withinTransaction(readOp -> {
                readOp.relationshipCursorGetAll().forAll(relationshipItem -> {
                    final long nodeId = relationshipItem.startNode();
                    builder.add(idMapping.toMappedNodeId(nodeId),
                            idMapping.toMappedNodeId(relationshipItem.otherNode(nodeId)));
                });
            });
            return builder.build();
        }
    }
}
