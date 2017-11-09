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
package org.neo4j.graphalgo.impl.msbfs;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public final class MultiBitSet32Test extends RandomizedTest {

    @Test
    public void shouldSetSingleBitWithOr() {
        MultiBitSet32 bitSet = new MultiBitSet32(1);
        for (int i = 0; i < 32; i++) {
            bitSet.setBit(0, i);
            int expected = i == 31 ? -1 : ((1 << i + 1) - 1);
            assertEquals("" + i, expected, bitSet.get(0));
        }
    }

    @Test
    public void shouldSetAndOverwriteCompleteBits() {
        MultiBitSet32 bitSet = new MultiBitSet32(1);
        int i = randomInt();
        bitSet.set(0, i);
        assertEquals(i, bitSet.get(0));
    }

    @Test
    public void shouldIterateToNextSetBit() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        int node = between(1, 9);
        bitSet.set(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(0));
    }

    @Test
    public void shouldReturnStartNodeIfSet() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        int node = between(1, 9);
        bitSet.set(node, 42);
        assertEquals(node, bitSet.nextSetNodeId(node));
    }

    @Test
    public void shouldReturnMinusOneIfNoMoreSetBits() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        int node = between(1, 5);
        bitSet.set(node, 42);
        assertEquals(-1, bitSet.nextSetNodeId(node + 1));
    }

    @Test
    public void shouldReturnMinusOneIfEmptyButCheckedFromMiddle() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        assertEquals(-1, bitSet.nextSetNodeId(between(1, 8)));
    }

    @Test
    public void shouldReturnMinusTwoIfEmpty() {
        MultiBitSet32 bitSet = new MultiBitSet32(10);
        assertEquals(-2, bitSet.nextSetNodeId(0));
    }
}
