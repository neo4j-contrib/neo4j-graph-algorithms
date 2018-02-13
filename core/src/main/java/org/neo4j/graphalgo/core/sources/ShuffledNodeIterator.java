package org.neo4j.graphalgo.core.sources;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.NodeIterator;

import java.util.Random;
import java.util.function.IntPredicate;

/**
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
        return new ShuffledIterAdapter(iterator.nodeIterator(), shuffleSize, System.currentTimeMillis());
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
            shuffle();
            return offset > 0;
        }

        @Override
        public int next() {
            return data[--offset];
        }

        private void shuffle() {
            int t, rnd;
            for (int i = 0; i < offset; i++) {
                rnd = i + random.nextInt(offset - i);
                t = data[i];
                data[i] = data[rnd];
                data[rnd] = t;
            }
        }
    }
}
