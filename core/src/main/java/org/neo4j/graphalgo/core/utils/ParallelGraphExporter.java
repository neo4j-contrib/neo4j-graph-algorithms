package org.neo4j.graphalgo.core.utils;

public interface ParallelGraphExporter {

    /**
     * Return a new {@link GraphExporter}.
     * This method is called on each Thread that performs the exporting,
     * possibly in concurrently, so this method must be thread-safe.
     * Depending on the batch size and the number of available threads,
     * the same thread may call this method multiple times.
     * If this methods returns same instances across multiple calls from multiple
     * threads, those instances have to be thread-safe as well.
     */
    GraphExporter newExporter();

    @FunctionalInterface
    interface Simple extends GraphExporter, ParallelGraphExporter {
        @Override
        default GraphExporter newExporter() {
            return this;
        }
    }
}
