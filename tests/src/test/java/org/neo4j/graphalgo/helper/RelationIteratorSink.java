package org.neo4j.graphalgo.helper;


import org.neo4j.graphalgo.api.RelationConsumer;
import org.neo4j.graphalgo.api.RelationCursor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;


/**
 * @author mknobloch
 */
public class RelationIteratorSink implements RelationConsumer {

    private static class E {

        int sourceId;
        int targetId;

        public E(int sourceId, int targetId) {
            this.sourceId = sourceId;
            this.targetId = targetId;
        }

        @Override
        public String toString() {
            return sourceId + "->" + targetId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            E e = (E) o;

            if (sourceId != e.sourceId) return false;
            return targetId == e.targetId;

        }

        @Override
        public int hashCode() {
            int result = sourceId;
            result = 31 * result + targetId;
            return result;
        }

    }
    private final Set<E> data = new HashSet<>();

    public RelationIteratorSink consume(Iterator<RelationCursor> iterator) {

        while (iterator.hasNext()) {
            final RelationCursor next = iterator.next();
            data.add(new E(next.sourceNodeId, next.targetNodeId));
        }
        return this;
    }

    public RelationIteratorSink assertContains(int sourceNodeId, int targetNodeId) {
        if (!data.contains(new E(sourceNodeId, targetNodeId))) {
            throw new AssertionError(sourceNodeId + " -> " + targetNodeId + " not element of data " + data);
        }
        return this;
    }


    public RelationIteratorSink reset() {
        data.clear();
        return this;
    }


    public RelationIteratorSink assertSize(int expected) {
        assertEquals(data.toString(), expected, data.size());
        return this;
    }

    @Override
    public void accept(int sourceNodeId, int targetNodeId, long relationId) {
        data.add(new E(sourceNodeId, targetNodeId));
    }


}
