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
package org.neo4j.graphalgo.core.utils.container;

import com.carrotsearch.hppc.BitMixer;
import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.HashOrderMixingStrategy;
import org.neo4j.graphalgo.core.utils.paged.BitUtil;

public final class LongLongDoubleHashMap {

    private long[] keys1;
    private long[] keys2;
    private double[] values;
    private boolean hasEmptyKey;

    private int keyMixer;
    private int assigned;
    private int mask;
    private HashOrderMixingStrategy orderMixer;

    public LongLongDoubleHashMap(int expectedElements) {
        this(expectedElements, HashOrderMixing.randomized());
    }

    private LongLongDoubleHashMap(
            int expectedElements,
            HashOrderMixingStrategy orderMixer) {
        this.orderMixer = orderMixer;
        allocateBuffers(BitUtil.nextHighestPowerOfTwo(expectedElements) << 1);
    }

    public double put(long key1, long key2, double value) {
        assert assigned < mask + 1;

        final int mask = this.mask;
        if (key1 == 0 && key2 == 0) {
            hasEmptyKey = true;
            double previousValue = values[mask + 1];
            values[mask + 1] = value;
            return previousValue;
        } else {
            final long[] keys1 = this.keys1;
            final long[] keys2 = this.keys2;
            if (key1 == 0) {
                key1 = -1;
            }
            if (key2 == 0) {
                key2 = -1;
            }
            int slot = hashKeys(key1, key2) & mask;

            long existing;
            while (!((existing = keys1[slot]) == 0)) {
                if (existing == key1 && keys2[slot] == key2) {
                    final double previousValue = values[slot];
                    values[slot] = value;
                    return previousValue;
                }
                slot = (slot + 1) & mask;
            }

            keys1[slot] = key1;
            keys2[slot] = key2;
            values[slot] = value;

            assigned++;
            return 0d;
        }
    }

    public double getOrDefault(long key1, long key2, double defaultValue) {
        if (key1 == 0 && key2 == 0) {
            return hasEmptyKey ? values[mask + 1] : defaultValue;
        } else {
            final long[] keys1 = this.keys1;
            final long[] keys2 = this.keys2;
            final int mask = this.mask;
            if (key1 == 0) {
                key1 = -1;
            }
            if (key2 == 0) {
                key2 = -1;
            }
            int slot = hashKeys(key1, key2) & mask;

            long existing;
            while (!((existing = keys1[slot]) == 0)) {
                if (existing == key1 && keys2[slot] == key2) {
                    return values[slot];
                }
                slot = (slot + 1) & mask;
            }

            return defaultValue;
        }
    }

    public void release() {
        assigned = 0;
        hasEmptyKey = false;
        keys1 = null;
        keys2 = null;
        values = null;
    }

    private int hashKeys(long key1, long key2) {
        long mix1 = BitMixer.mix(key1, keyMixer);
        long mix2 = BitMixer.mix(key2, keyMixer) & 0xFFFFFFFFL;
        return BitMixer.mix(mix1 << 32 | mix2, keyMixer);
    }

    private void allocateBuffers(int arraySize) {
        assert Integer.bitCount(arraySize) == 1;
        final int newKeyMixer = orderMixer.newKeyMixer(arraySize);
        try {
            this.keys1 = new long[arraySize];
            this.keys2 = new long[arraySize];
            this.values = new double[arraySize + 1];
        } catch (OutOfMemoryError e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Not enough memory to allocate for %d sized buffers",
                            arraySize),
                    e);
        }
        this.keyMixer = newKeyMixer;
        this.mask = arraySize - 1;
    }
}
