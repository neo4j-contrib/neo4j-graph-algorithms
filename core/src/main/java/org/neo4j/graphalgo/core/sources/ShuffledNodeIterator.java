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
    private final Random random;

    private final int shuffleSize;

    public ShuffledNodeIterator(NodeIterator iterator, int shuffleSize) {
        this.iterator = iterator;
        this.shuffleSize = shuffleSize;
        this.random = new Random();
    }

    public ShuffledNodeIterator(NodeIterator iterator, int shuffleSize, long seed) {
        this.iterator = iterator;
        this.shuffleSize = shuffleSize;
        this.random = new Random(seed);
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
        return new ShuffledIterAdapter(iterator.nodeIterator(), shuffleSize);
    }

    private class ShuffledIterAdapter implements PrimitiveIntIterator {

        private final PrimitiveIntIterator parent;
        private final int[] data;
        private int offset = 0;

        private ShuffledIterAdapter(PrimitiveIntIterator parent, int rndSize) {
            this.parent = parent;
            this.data = new int[rndSize];
        }

        @Override
        public boolean hasNext() {
            if (offset > 0) {
                return true;
            }
            while (parent.hasNext() && offset < data.length) {
                data[offset++] = parent.next();
            }
            shuffle(data, offset);
            return offset > 0;
        }

        @Override
        public int next() {
            return data[--offset];
        }
    }

    private void shuffle(int[] a, int length) {
        random.nextInt();
        int t;
        for (int i = 0; i < length; i++) {
            int change = i + random.nextInt(length - i);
            t = a[i];
            a[i] = a[change];
            a[change] = t;
        }
    }
}
