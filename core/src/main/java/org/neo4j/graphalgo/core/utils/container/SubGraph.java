package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.LongArrayDeque;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.predicates.LongPredicate;
import org.neo4j.graphalgo.api.AllRelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Iterator;

/**
 * @author mknblch
 */
public class SubGraph implements AllRelationshipIterator {

    private final LongArrayDeque deque;

    public SubGraph(int expectedElements) {
        deque = new LongArrayDeque(expectedElements);
    }

    public void add(long combinedSourceTarget) {
        deque.addLast(combinedSourceTarget);
    }

    public void add(int sourceNodeId, int targetNodeId) {
        deque.addLast(RawValues.combineIntInt(sourceNodeId, targetNodeId));
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        forEachRelationship((source, target, relationId) -> {
            builder.append(source).append("->").append(target).append("\n");
            return true;
        });
        return builder.toString();
    }

    @Override
    public void forEachRelationship(RelationshipConsumer consumer) {
        deque.forEach((LongPredicate) value ->
                consumer.accept(RawValues.getHead(value), RawValues.getTail(value), -1L));
    }

    @Override
    public Iterator<RelationshipCursor> allRelationshipIterator() {
        return new SGIterator(deque.iterator());
    }

    private static class SGIterator implements Iterator<RelationshipCursor> {

        private final Iterator<LongCursor> iterator;
        private final RelationshipCursor cursor = new RelationshipCursor();

        private SGIterator(Iterator<LongCursor> iterator) {
            this.iterator = iterator;
            cursor.relationshipId = -1L;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public RelationshipCursor next() {
            final LongCursor longCursor = iterator.next();
            cursor.sourceNodeId = RawValues.getHead(longCursor.value);
            cursor.targetNodeId = RawValues.getTail(longCursor.value);
            return cursor;
        }
    }
}
