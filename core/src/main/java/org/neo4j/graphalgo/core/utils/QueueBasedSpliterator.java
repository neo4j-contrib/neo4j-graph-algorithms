package org.neo4j.graphalgo.core.utils;

import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author mh
 * @since 07.07.18
 */
public class QueueBasedSpliterator<T> implements Spliterator<T> {
    private final BlockingQueue<T> queue;
    private T tombstone;
    private T entry;
    private TerminationFlag terminationGuard;
    private final int timeout;

    public QueueBasedSpliterator(BlockingQueue<T> queue, T tombstone, TerminationFlag terminationGuard) {
        this(queue, tombstone, terminationGuard, 10);
    }

    public QueueBasedSpliterator(BlockingQueue<T> queue, T tombstone, TerminationFlag terminationGuard, int timeout) {
        this.queue = queue;
        this.tombstone = tombstone;
        this.terminationGuard = terminationGuard;
        this.timeout = timeout;
        entry = poll();
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (!terminationGuard.running() || isEnd()) return false;
        action.accept(entry);
        entry = poll();
        return !isEnd();
    }

    private boolean isEnd() {
        return entry == null || entry == tombstone;
    }

    private T poll() {
        try {
            return queue.poll(timeout, SECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public Spliterator<T> trySplit() {
        return null;
    }

    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    public int characteristics() {
        return NONNULL;
    }

    public void offer(T items) {
        try {
            queue.offer(items, timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
