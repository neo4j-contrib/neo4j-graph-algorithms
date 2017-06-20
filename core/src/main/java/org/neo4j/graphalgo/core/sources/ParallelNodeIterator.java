package org.neo4j.graphalgo.core.sources;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.IdMapping;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.core.utils.ParallelUtil;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.*;

/**
 * @author mknblch
 */
public class ParallelNodeIterator {

    private final ExecutorService executorService;
    private final int nodeCount;
    private final int concurrency;
    private final ArrayList<Future<?>> futures;

    public ParallelNodeIterator(IdMapping idMapping, ExecutorService executorService, int concurrency) {
        this.executorService = executorService;
        nodeCount = idMapping.nodeCount();
        this.concurrency = concurrency;
        futures = new ArrayList<>();
    }

    public ParallelNodeIterator compute(Supplier<Consumer<PrimitiveIntIterator>> consumerSupplier) {

        final int batchSize = nodeCount / concurrency;
        futures.clear();
        for (int i = 0; i < nodeCount; i += batchSize) {
            final BatchIterator it = new BatchIterator(i, Math.min(nodeCount, i + batchSize));
            final Consumer<PrimitiveIntIterator> consumer = consumerSupplier.get();
            futures.add(executorService.submit(() -> consumer.accept(it)));
        }
        return this;
    }

    public ParallelNodeIterator awaitTermination() {
        ParallelUtil.awaitTermination(futures);
        return this;
    }

    private static class BatchIterator implements PrimitiveIntIterator {

        private int offset;
        private final int end;

        private BatchIterator(int offset, int end) {
            this.offset = offset;
            this.end = end;
        }

        @Override
        public boolean hasNext() {
            return offset < end;
        }

        @Override
        public int next() {
            return offset++;
        }
    };
}
