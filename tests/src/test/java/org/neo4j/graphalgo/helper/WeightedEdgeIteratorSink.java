package org.neo4j.graphalgo.helper;


import org.neo4j.graphalgo.api.WeightedEdgeConsumer;
import org.neo4j.graphalgo.api.WeightedEdgeCursor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;


/**
 * @author mknobloch
 */
public class WeightedEdgeIteratorSink implements WeightedEdgeConsumer {

    public static final int MULTIPLIER = 1000000;

    private static class E {

        int sourceId;
        int targetId;
        int weight;

        public E(int sourceId, int targetId, double weight) {
            this.sourceId = sourceId;
            this.targetId = targetId;
            this.weight = (int) (weight * MULTIPLIER);
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
            if (targetId != e.targetId) return false;
            return weight == e.weight;

        }

        @Override
        public int hashCode() {
            int result = sourceId;
            result = 31 * result + targetId;
            result = 31 * result + weight;
            return result;
        }
    }
    private final Set<E> data = new HashSet<>();

    public WeightedEdgeIteratorSink consume(Iterator<WeightedEdgeCursor> iterator) {

        while (iterator.hasNext()) {
            final WeightedEdgeCursor next = iterator.next();
            data.add(new E(next.sourceVertexId, next.targetVertexId, next.weight));
        }
        return this;
    }

    public WeightedEdgeIteratorSink assertContains(int sourceVertexId, int targetVertexId, double weight) {
        if (!data.contains(new E(sourceVertexId, targetVertexId, weight))) {
            throw new AssertionError(sourceVertexId + " -> " + targetVertexId + " not element of data " + data);
        }
        return this;
    }


    public WeightedEdgeIteratorSink reset() {
        data.clear();
        return this;
    }


    public WeightedEdgeIteratorSink assertSize(int expected) {
        assertEquals(data.toString(), expected, data.size());
        return this;
    }

    @Override
    public void accept(int sourceVertexId, int targetVertexId, long edgeId, double weight) {
        data.add(new E(sourceVertexId, targetVertexId, weight));
    }

}
