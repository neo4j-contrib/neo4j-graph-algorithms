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
package org.neo4j.graphalgo.core.sources;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.NodeIterator;

import java.util.Random;
import java.util.function.IntPredicate;

/**
 * NodeIterator adapter with randomized order
 *
 * @author mknblch
 */
public class ShuffledNodeIterator implements NodeIterator {

    private final NodeIterator iterator;
    private final int shuffleSize;

    public ShuffledNodeIterator(NodeIterator iterator, int shuffleSize) {
        this.iterator = iterator;
        this.shuffleSize = shuffleSize;
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        final PrimitiveIntIterator nodeIterator = nodeIterator();
        while (nodeIterator.hasNext()) {
            if (!consumer.test(nodeIterator.next())) {
                break;
            }
        }
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return new ShuffledIterAdapter(iterator.nodeIterator(), shuffleSize, System.nanoTime());
    }

    public PrimitiveIntIterator nodeIterator(long seed) {
        return new ShuffledIterAdapter(iterator.nodeIterator(), shuffleSize, seed);
    }

    private class ShuffledIterAdapter implements PrimitiveIntIterator {

        private final PrimitiveIntIterator parent;
        private final int[] data;
        private int offset = 0;
        private final Random random;

        private ShuffledIterAdapter(PrimitiveIntIterator parent, int rndSize, long seed) {
            this.parent = parent;
            this.data = new int[rndSize];
            this.random = new Random(seed);
        }

        @Override
        public boolean hasNext() {
            if (offset > 0) {
                return true;
            }
            while (parent.hasNext() && offset < data.length) {
                data[offset++] = parent.next();
            }
            shuffle(data, offset, random);
            return offset > 0;
        }

        @Override
        public int next() {
            return data[--offset];
        }

    }

    private static void shuffle(int[] data, int length, Random rnd) {
        int t, r;
        for (int i = 0; i < length; i++) {
            r = i + rnd.nextInt(length - i);
            t = data[i];
            data[i] = data[r];
            data[r] = t;
        }
    }
}
