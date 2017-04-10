package org.neo4j.graphalgo.core.sources;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.graphalgo.api.AllRelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * Buffered AllRelationshipIterator based on hppc container
 *
 * @author mknblch
 */
public class BufferedAllRelationshipIterator implements AllRelationshipIterator {

    private final LongArrayList container;

    public void add(int sourceNodeId, int targetNodeId) {
        container.add(RawValues.combineIntInt(sourceNodeId, targetNodeId));
    }

    public BufferedAllRelationshipIterator(int expectedElements) {
        container = new LongArrayList(expectedElements);
    }

    @Override
    public void forEachRelationship(RelationshipConsumer consumer) {
        container.forEach((Consumer<LongCursor>) longCursor ->
                consumer.accept(
                        RawValues.getHead(longCursor.value),
                        RawValues.getTail(longCursor.value),
                        -1L));
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
}
