package org.neo4j.graphalgo.helper;


import org.neo4j.graphalgo.api.WeightedRelationConsumer;
import org.neo4j.graphalgo.api.WeightedRelationCursor;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;


/**
 * @author mknobloch
 */
public class WeightedRelationIteratorSink implements WeightedRelationConsumer {

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

    public WeightedRelationIteratorSink consume(Iterator<WeightedRelationCursor> iterator) {

        while (iterator.hasNext()) {
            final WeightedRelationCursor next = iterator.next();
            data.add(new E(next.sourceNodeId, next.targetNodeId, next.weight));
        }
        return this;
    }

    public WeightedRelationIteratorSink assertContains(int sourceNodeId, int targetNodeId, double weight) {
        if (!data.contains(new E(sourceNodeId, targetNodeId, weight))) {
            throw new AssertionError(sourceNodeId + " -> " + targetNodeId + " not element of data " + data);
        }
        return this;
    }


    public WeightedRelationIteratorSink reset() {
        data.clear();
        return this;
    }


    public WeightedRelationIteratorSink assertSize(int expected) {
        assertEquals(data.toString(), expected, data.size());
        return this;
    }

    @Override
    public void accept(int sourceNodeId, int targetNodeId, long relationId, double weight) {
        data.add(new E(sourceNodeId, targetNodeId, weight));
    }

}
