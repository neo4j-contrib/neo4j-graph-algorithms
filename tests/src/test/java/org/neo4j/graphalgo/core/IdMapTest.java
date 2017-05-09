package org.neo4j.graphalgo.core;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Collection;
import java.util.function.IntToLongFunction;

import static org.junit.Assert.*;

public final class IdMapTest extends RandomizedTest {

    @Test
    public void shouldReturnSingleIteratorForLargeBatchSize() throws Exception {
        IdMap idMap = new IdMap(20);
        long[] ids = addRandomIds(idMap);
        idMap.buildMappedIds();

        Collection<PrimitiveIntIterable> iterables = idMap.batchIterables(100);
        assertEquals(1, iterables.size());

        assertIterables(idMap, ids, iterables);
    }

    @Test
    public void shouldReturnMultipleIteratorsForSmallBatchSize() throws Exception {
        IdMap idMap = new IdMap(20);
        long[] ids = addRandomIds(idMap);
        idMap.buildMappedIds();

        int expectedBatches = ids.length / 3;
        if (ids.length % 3 != 0) {
            expectedBatches++;
        }

        Collection<PrimitiveIntIterable> iterables = idMap.batchIterables(3);
        assertEquals(expectedBatches, iterables.size());

        assertIterables(idMap, ids, iterables);
    }

    @Test
    public void shouldFailForZeroBatchSize() throws Exception {
        IdMap idMap = new IdMap(20);
        addRandomIds(idMap);
        idMap.buildMappedIds();

        try {
            idMap.batchIterables(0);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: 0", e.getMessage());
        }
    }

    @Test
    public void shouldFailForNegativeBatchSize() throws Exception {
        IdMap idMap = new IdMap(20);
        addRandomIds(idMap);
        idMap.buildMappedIds();

        int batchSize = between(Integer.MIN_VALUE, -1);

        try {
            idMap.batchIterables(batchSize);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: " + batchSize, e.getMessage());
        }
    }

    private void assertIterables(
            final IdMap idMap,
            final long[] ids,
            final Collection<PrimitiveIntIterable> iterables) {
        int i = 0;
        for (PrimitiveIntIterable iterable : iterables) {
            PrimitiveIntIterator iterator = iterable.iterator();
            while (iterator.hasNext()) {
                int next = iterator.next();
                long id = ids[i];
                assertEquals(i++, next);
                assertEquals(id, idMap.unmap(next));
            }
        }
    }

    private long[] addRandomIds(final IdMap idMap) {
        LongHashSet seen = new LongHashSet();
        return addSomeIds(idMap, i -> {
            long id;
            do {
                id = between(42L, 1337L);
            } while (!seen.add(id));
            return id;
        });
    }

    private long[] addSomeIds(final IdMap idMap, IntToLongFunction newId) {
        int iterations = between(10, 20);
        long[] ids = new long[iterations];
        for (int i = 0; i < iterations; i++) {
            ids[i] = newId.applyAsLong(i);
            idMap.add(ids[i]);
        }
        return ids;
    }
}
