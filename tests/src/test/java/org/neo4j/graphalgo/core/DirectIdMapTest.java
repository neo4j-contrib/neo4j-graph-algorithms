/**
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core;

import org.junit.Test;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;

import java.util.Collection;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public final class DirectIdMapTest {

    private int size = 20;

    private IntStream ids() {
        return IntStream.range(0, size);
    }

    @Test
    public void basicTest() {
        DirectIdMap idMap = new DirectIdMap(size);
        assertEquals(size, idMap.nodeCount());
        assertTrue(ids().allMatch(idMap::contains));
        assertTrue(IntStream.range(-100,0).noneMatch(idMap::contains));
        assertTrue(IntStream.range(size,100).noneMatch(idMap::contains));

        assertTrue(ids().allMatch(i -> idMap.toMappedNodeId(i) == i));
        assertTrue(ids().allMatch(i -> idMap.toOriginalNodeId(i) == i));

        PrimitiveIntIterator it = idMap.nodeIterator();
        assertTrue(ids().allMatch(i -> it.next() == i));
    }

    @Test
    public void shouldReturnSingleIteratorForLargeBatchSize() {
        DirectIdMap idMap = new DirectIdMap(size);

        Collection<PrimitiveIntIterable> iterables = idMap.batchIterables(100);
        assertEquals(1, iterables.size());

        assertIterables(idMap, ids().toArray(), iterables);
    }

    @Test
    public void shouldReturnMultipleIteratorsForSmallBatchSize() {
        DirectIdMap idMap = new DirectIdMap(size);

        int expectedBatches = size / 3 + (size % 3 > 0 ? 1 :0);

        Collection<PrimitiveIntIterable> iterables = idMap.batchIterables(3);
        assertEquals(expectedBatches, iterables.size());

        assertIterables(idMap, ids().toArray(), iterables);
    }

    @Test
    public void shouldFailForZeroBatchSize() {
        DirectIdMap idMap = new DirectIdMap(0);

        try {
            idMap.batchIterables(0);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: 0", e.getMessage());
        }
    }

    @Test
    public void shouldFailForNegativeBatchSize() {
        DirectIdMap idMap = new DirectIdMap(size);

        int batchSize = -10;

        try {
            idMap.batchIterables(batchSize);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid batch size: " + batchSize, e.getMessage());
        }
    }

    private void assertIterables(
            final IdMap idMap,
            final int[] ids,
            final Collection<PrimitiveIntIterable> iterables) {
        int i = 0;
        for (PrimitiveIntIterable iterable : iterables) {
            PrimitiveIntIterator iterator = iterable.iterator();
            while (iterator.hasNext()) {
                int next = iterator.next();
                long id = ids[i];
                assertEquals(i++, next);
                assertEquals(id, idMap.toOriginalNodeId(next));
            }
        }
    }
}
