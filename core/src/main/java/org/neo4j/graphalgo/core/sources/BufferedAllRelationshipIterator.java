package org.neo4j.graphalgo.core.sources;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.graphalgo.api.AllRelationshipIterator;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.Importer;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Iterator;
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

    @Override
    public Iterator<RelationshipCursor> allRelationshipIterator() {
        return new AllIterator(container.iterator());
    }

    private static class AllIterator implements Iterator<RelationshipCursor> {

        private final Iterator<LongCursor> iterator;
        private final RelationshipCursor relationCursor = new RelationshipCursor();

        public AllIterator(Iterator<LongCursor> iterator) {
            this.iterator = iterator;
            relationCursor.relationshipId = -1;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public RelationshipCursor next() {
            final LongCursor cursor = iterator.next();
            relationCursor.sourceNodeId = RawValues.getHead(cursor.value);
            relationCursor.targetNodeId = RawValues.getTail(cursor.value);
            return relationCursor;
        }
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
