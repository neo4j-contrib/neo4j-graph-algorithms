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
package org.neo4j.graphalgo.impl;

import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphdb.Direction;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public abstract class LabelPropagationAlgorithm<Self extends LabelPropagationAlgorithm<Self>> extends Algorithm<Self> {

    public static final String PARTITION_TYPE = "property";
    public static final String WEIGHT_TYPE = "weight";

    public static class StreamResult {
        public final long nodeId;
        public final long label;

        public StreamResult(long nodeId, long label) {
            this.nodeId = nodeId;
            this.label = label;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final Self me() {
        return (Self) this;
    }

    public final Self compute(Direction direction, long maxIterations) {
        return compute(direction, maxIterations, true);
    }

    abstract Self compute(
            Direction direction,
            long maxIterations,
            boolean randomizeOrder);

    public abstract long ranIterations();

    public abstract boolean didConverge();

    public abstract Labels labels();

    public interface Labels {
        long labelFor(long nodeId);

        long size();
    }

    public static final PropertyTranslator.OfLong<Labels> LABEL_TRANSLATOR =
            Labels::labelFor;

    static final class LabelArray implements Labels {
        final int[] labels;

        LabelArray(final int[] labels) {
            this.labels = labels;
        }

        @Override
        public long labelFor(final long nodeId) {
            return (long) labels[Math.toIntExact(nodeId)];
        }

        @Override
        public long size() {
            return (long) labels.length;
        }
    }

    static final class HugeLabelArray implements Labels {
        final HugeLongArray labels;

        HugeLabelArray(final HugeLongArray labels) {
            this.labels = labels;
        }

        @Override
        public long labelFor(final long nodeId) {
            return labels.get(nodeId);
        }

        @Override
        public long size() {
            return labels.size();
        }
    }

    static final class RandomlySwitchingIntIterable implements PrimitiveIntIterable {
        private final PrimitiveIntIterable delegate;
        private final Random random;

        static PrimitiveIntIterable of(
                boolean randomize,
                PrimitiveIntIterable delegate) {
            return randomize
                    ? new RandomlySwitchingIntIterable(delegate, ThreadLocalRandom.current())
                    : delegate;
        }

        private RandomlySwitchingIntIterable(PrimitiveIntIterable delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public PrimitiveIntIterator iterator() {
            return new RandomlySwitchingIntIterator(delegate.iterator(), random);
        }
    }

    static final class RandomlySwitchingIntIterator implements PrimitiveIntIterator {
        private final PrimitiveIntIterator delegate;
        private final Random random;
        private boolean hasSkipped;
        private int skipped;

        private RandomlySwitchingIntIterator(PrimitiveIntIterator delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public boolean hasNext() {
            return hasSkipped || delegate.hasNext();
        }

        @Override
        public int next() {
            if (hasSkipped) {
                int elem = skipped;
                hasSkipped = false;
                return elem;
            }
            int next = delegate.next();
            if (delegate.hasNext() && random.nextBoolean()) {
                skipped = next;
                hasSkipped = true;
                return delegate.next();
            }
            return next;
        }
    }

    static final class RandomlySwitchingLongIterable implements PrimitiveLongIterable {
        private final PrimitiveLongIterable delegate;
        private final Random random;

        static PrimitiveLongIterable of(
                boolean randomize,
                PrimitiveLongIterable delegate) {
            return randomize
                    ? new RandomlySwitchingLongIterable(delegate, ThreadLocalRandom.current())
                    : delegate;
        }

        private RandomlySwitchingLongIterable(PrimitiveLongIterable delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public PrimitiveLongIterator iterator() {
            return new RandomlySwitchingLongIterator(delegate.iterator(), random);
        }
    }

    static final class RandomlySwitchingLongIterator implements PrimitiveLongIterator {
        private final PrimitiveLongIterator delegate;
        private final Random random;
        private boolean hasSkipped;
        private long skipped;

        private RandomlySwitchingLongIterator(PrimitiveLongIterator delegate, Random random) {
            this.delegate = delegate;
            this.random = random;
        }

        @Override
        public boolean hasNext() {
            return hasSkipped || delegate.hasNext();
        }

        @Override
        public long next() {
            if (hasSkipped) {
                long elem = skipped;
                hasSkipped = false;
                return elem;
            }
            long next = delegate.next();
            if (delegate.hasNext() && random.nextBoolean()) {
                skipped = next;
                hasSkipped = true;
                return delegate.next();
            }
            return next;
        }
    }
}
