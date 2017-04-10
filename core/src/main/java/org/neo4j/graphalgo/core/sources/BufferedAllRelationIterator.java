package org.neo4j.graphalgo.core.sources;

import com.carrotsearch.hppc.LongArrayDeque;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongContainer;
import com.carrotsearch.hppc.cursors.LongCursor;
import org.neo4j.graphalgo.api.AllRelationIterator;
import org.neo4j.graphalgo.api.RelationConsumer;
import org.neo4j.graphalgo.api.RelationCursor;
import org.neo4j.graphalgo.core.utils.RawValues;

import java.util.Iterator;
import java.util.function.Consumer;

/**
 * @author mknblch
 */
public class BufferedAllRelationIterator implements AllRelationIterator {

    private final LongArrayList container;

    public void add(int sourceNodeId, int targetNodeId) {
        container.add(RawValues.combineIntInt(sourceNodeId, targetNodeId));
    }

    public BufferedAllRelationIterator(int expectedElements) {
        container = new LongArrayList(expectedElements);
    }

    @Override
    public void forEachRelation(RelationConsumer consumer) {
        container.forEach((Consumer<LongCursor>) longCursor ->
                consumer.accept(
                        RawValues.getHead(longCursor.value),
                        RawValues.getTail(longCursor.value),
                        -1L));
    }

    @Override
    public Iterator<RelationCursor> allRelationIterator() {
        return new AllIterator(container.iterator());
    }

    private static class AllIterator implements Iterator<RelationCursor> {

        private final Iterator<LongCursor> iterator;
        private final RelationCursor relationCursor = new RelationCursor();

        public AllIterator(Iterator<LongCursor> iterator) {
            this.iterator = iterator;
            relationCursor.relationId = -1;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public RelationCursor next() {
            final LongCursor cursor = iterator.next();
            relationCursor.sourceNodeId = RawValues.getHead(cursor.value);
            relationCursor.targetNodeId = RawValues.getTail(cursor.value);
            return relationCursor;
        }
    }
}
