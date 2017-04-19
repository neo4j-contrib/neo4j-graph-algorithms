package org.neo4j.graphalgo.core.utils.queue;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;
import org.neo4j.graphalgo.core.utils.queue.LongMinPriorityQueue;
import org.neo4j.helpers.collection.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.*;


public final class LongMinPriorityQueueTest extends RandomizedTest {

    @Test
    public void testIsEmpty() throws Exception {
        final int capacity = RandomizedTest.between(10, 20);
        final LongMinPriorityQueue queue = new LongMinPriorityQueue(capacity);
        assertEquals(queue.size(), 0);
    }

    @Test
    public void testClear() throws Exception {
        final int maxSize = RandomizedTest.between(3, 10);
        final LongMinPriorityQueue queue = new LongMinPriorityQueue(maxSize);
        final int iterations = RandomizedTest.between(3, maxSize);
        for (int i = 0; i < iterations; i++) {
            queue.add(i, RandomizedTest.between(1, 5));
        }
        assertEquals(queue.size(), iterations);
        queue.clear();
        assertEquals(queue.size(), 0);
    }

    @Test
    public void testGrowing() throws Exception {
        final int maxSize = RandomizedTest.between(10, 20);
        final LongMinPriorityQueue queue = new LongMinPriorityQueue(1);
        for (int i = 0; i < maxSize; i++) {
            queue.add(i, RandomizedTest.randomIntBetween(1, 5));
        }
        assertEquals(queue.size(), maxSize);
    }

    @Test
    public void testAdd() throws Exception {
        final int iterations = RandomizedTest.between(5, 50);
        final LongMinPriorityQueue queue = new LongMinPriorityQueue();
        int min = -1;
        double minWeight = Double.POSITIVE_INFINITY;
        for (int i = 0; i < iterations; i++) {
            final double weight = exclusiveDouble(0d, 100d);
            if (weight < minWeight) {
                minWeight = weight;
                min = i;
            }
            assertEquals(queue.add(i, weight), min);
        }
    }

    @Test
    public void testAddAndPop() throws Exception {
        final LongMinPriorityQueue queue = new LongMinPriorityQueue();
        final List<Pair<Long, Double>> elements = new ArrayList<>();

        final int iterations = RandomizedTest.between(5, 50);
        long min = -1;
        double minWeight = Double.POSITIVE_INFINITY;
        for (long i = 1; i <= iterations; i++) {
            final double weight = exclusiveDouble(0d, 100d);
            if (weight < minWeight) {
                minWeight = weight;
                min = i;
            }
            assertEquals(queue.add(i, weight), min);
            elements.add(Pair.of(i, weight));
        }

        // PQ isn't stable for duplicate elements, so we have to
        // test those with non strict ordering requirements
        final Map<Double, Set<Long>> byWeight = elements
                .stream()
                .collect(Collectors.groupingBy(
                        Pair::other,
                        Collectors.mapping(Pair::first, Collectors.toSet())));
        final List<Double> weightGroups = byWeight
                .keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());

        for (Double weight : weightGroups) {
            final Set<Long> allowedIds = byWeight.get(weight);
            while (!allowedIds.isEmpty()) {
                final long item = queue.pop();
                assertThat(allowedIds, hasItem(item));
                allowedIds.remove(item);
            }
        }

        assertTrue(queue.isEmpty());
    }

    private double exclusiveDouble(
            final double exclusiveMin,
            final double exclusiveMax) {
        return RandomizedTest.biasedDoubleBetween(
                Math.nextUp(exclusiveMin),
                Math.nextDown(exclusiveMax));
    }
}
