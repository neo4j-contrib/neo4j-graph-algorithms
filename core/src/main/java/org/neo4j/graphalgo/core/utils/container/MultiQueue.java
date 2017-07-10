package org.neo4j.graphalgo.core.utils.container;

import org.apache.lucene.util.ArrayUtil;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

/**
 * @author mknblch
 */
public class MultiQueue {

    private final AtomicIntegerArray offsets;
    private final int[][] queue;
    private final int nodeCount;
    private final ExecutorService executorService;


    public MultiQueue(ExecutorService executorService, int nodeCount) {
        this.executorService = executorService;
        this.nodeCount = nodeCount;
        offsets = new AtomicIntegerArray(nodeCount);
        queue = new int[nodeCount][];
    }

    public AtomicIntegerArray getOffsets() {
        return offsets;
    }

    public void clear(int phase) {
        offsets.set(phase, 0);
    }

    public void addOrCreate(int phase, int element) {

        if (offsets.get(phase) != 0) {
            final int offset = offsets.getAndIncrement(phase);
            queue[phase] = ArrayUtil.grow(queue[phase], offset + 1);
            queue[phase][offset] = element;
            return;
        }


        boolean done = false;
        while (!done) {
            int current = offsets.get(phase);
            done = offsets.compareAndSet(phase, current, current + 1);

            if (current == 0) {
                queue[phase] = new int[1];
            }
            queue[phase] = ArrayUtil.grow(queue[phase], current + 1);
            queue[phase][current] = element;
        }

    }

    public void forEach(int phase, IntConsumer consumer) {
        final int length = offsets.get(phase);
        for (int i = 0; i < length; i++) {
            consumer.accept(queue[phase][i]);
        }
    }

    public void forEach(Collection<Future<?>> futures, int phase, IntConsumer consumer) {

        futures.add(executorService.submit(() -> {
            final int length = offsets.get(phase);
            for (int i = 0; i < length; i++) {
                consumer.accept(queue[phase][i]);
            }
        }));
    }
}
