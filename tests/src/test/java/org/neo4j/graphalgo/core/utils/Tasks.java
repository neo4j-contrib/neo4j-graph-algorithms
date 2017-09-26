package org.neo4j.graphalgo.core.utils;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.stream.IntStream;


final class Tasks extends AbstractCollection<Runnable> {
    private final AtomicInteger started;
    private final AtomicInteger running;
    private final AtomicInteger requested;
    private final LongAccumulator maxRunning;
    private int size;
    private final long parkNanos;

    Tasks(int size) {
        this.size = size;
        started = new AtomicInteger();
        running = new AtomicInteger();
        requested = new AtomicInteger();
        maxRunning = new LongAccumulator(Long::max, Long.MIN_VALUE);
        parkNanos = 0;
    }

    Tasks(int size, int runtimeMillis) {
        this.size = size;
        started = new AtomicInteger();
        running = new AtomicInteger();
        requested = new AtomicInteger();
        maxRunning = new LongAccumulator(Long::max, Long.MIN_VALUE);
        parkNanos = TimeUnit.MILLISECONDS.toNanos(runtimeMillis);
    }

    @Override
    public Iterator<Runnable> iterator() {
        started.set(0);
        running.set(0);
        requested.set(0);
        maxRunning.reset();
        return new TrackingIterator<>(IntStream.range(0, size).mapToObj($ -> newTask()).iterator());
    }

    @Override
    public int size() {
        return size;
    }

    Tasks sized(int newSize) {
        size = newSize;
        return this;
    }

    int started() {
        return started.get();
    }

    int maxRunning() {
        return (int) maxRunning.get();
    }

    int requested() {
        return requested.get();
    }

    void run(Consumer<Tasks> block) {
        block.accept(this);
    }

    private Runnable newTask() {
        return () -> {
            maxRunning.accumulate(running.incrementAndGet());
            started.incrementAndGet();
            LockSupport.parkNanos(parkNanos);
            running.decrementAndGet();
        };
    }

    private final class TrackingIterator<T> implements Iterator<T> {
        private final Iterator<T> it;

        private TrackingIterator(Iterator<T> it) {
            this.it = it;
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public T next() {
            requested.incrementAndGet();
            return it.next();
        }
    }
}
