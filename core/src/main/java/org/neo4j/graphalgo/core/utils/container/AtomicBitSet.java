package org.neo4j.graphalgo.core.utils.container;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * https://stackoverflow.com/questions/12424633/atomicbitset-implementation-for-java
 */
public class AtomicBitSet {

    private final AtomicIntegerArray elements;

    public AtomicBitSet(int length) {
        elements = new AtomicIntegerArray((length + 31) >>> 5);
    }

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

    public boolean get(long n) {
        final int bit = 1 << n;
        final int index = (int) (n >>> 5);
        final int value = elements.get(index);
        return (value & bit) != 0;
    }
}