package org.neo4j.graphalgo.helper;


import org.neo4j.graphalgo.api.EdgeConsumer;
import org.neo4j.graphalgo.api.EdgeCursor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;


/**
 * @author mknobloch
 */
public class EdgeIteratorSink implements EdgeConsumer {

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

    public EdgeIteratorSink consume(Iterator<EdgeCursor> iterator) {

        while (iterator.hasNext()) {
            final EdgeCursor next = iterator.next();
            data.add(new E(next.sourceVertexId, next.targetVertexId));
        }
        return this;
    }

    public EdgeIteratorSink assertContains(int sourceVertexId, int targetVertexId) {
        if (!data.contains(new E(sourceVertexId, targetVertexId))) {
            throw new AssertionError(sourceVertexId + " -> " + targetVertexId + " not element of data " + data);
        }
        return this;
    }


    public EdgeIteratorSink reset() {
        data.clear();
        return this;
    }


    public EdgeIteratorSink assertSize(int expected) {
        assertEquals(data.toString(), expected, data.size());
        return this;
    }

    @Override
    public void accept(int sourceVertexId, int targetVertexId, long edgeId) {
        data.add(new E(sourceVertexId, targetVertexId));
    }


}
