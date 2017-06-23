package org.neo4j.graphalgo.core.utils.container;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * thread safe BitSet based on AtomicInts.
 * impl. taken from https://stackoverflow.com/questions/12424633/atomicbitset-implementation-for-java
 *
 */
public class AtomicBitSet {

    private final AtomicIntegerArray elements;

    public AtomicBitSet(int length) {
        elements = new AtomicIntegerArray((length + 31) >>> 5);
    }

    /**
     * reset the bitset
     */
    public void clear() {
        for (int i = elements.length() - 1; i >= 0; i--) {
            elements.set(i, 0);
        }
    }

    /**
     * set n
     * @param n
     */
    public void set(long n) {
        int bit = 1 << n;
        int index = (int) (n >>> 5);
        while (true) {
            int current = elements.get(index);
            int value = current | bit;
            if (current == value || elements.compareAndSet(index, current, value))
                return;
        }
    }

    /**
     * try to set n
     * @param n
     * @return true if successfully set n, false otherwise (another thread did it)
     */
    public boolean trySet(long n) {
        int bit = 1 << n;
        int index = (int) (n >>> 5);
        int current = elements.get(index);
        int value = current | bit;
        if (current == value || elements.compareAndSet(index, current, value)) {
            return true;
        }
        return false;
    }

    /**
     * unset n
     * @param n
     */
    public void unset(long n) {
        final int bit = ~(1 << n);
        final int index = (int) (n >>> 5);
        while (true) {
            final int current = elements.get(index);
            final int value = current & bit;
            if (current == value || elements.compareAndSet(index, current, value))
                return;
        }
    }

    /**
     * get state of bit n
     * @param n the biut
     * @return its state
     */
    public boolean get(long n) {
        final int bit = 1 << n;
        final int index = (int) (n >>> 5);
        final int value = elements.get(index);
        return (value & bit) != 0;
    }
}