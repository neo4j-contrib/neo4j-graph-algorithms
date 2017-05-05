package org.neo4j.graphalgo.core.utils;

import org.neo4j.collection.primitive.PrimitiveIntIterable;

public interface ParallelGraphImporter<T extends Runnable> {

    /**
     * Return a new {@link Runnable}.
     * This method is called on each Thread that performs the importing,
     * possibly in concurrently, so this method must be thread-safe.
     * Depending on the batch size and the number of available threads,
     * the same thread may call this method multiple times.
     * If this methods returns same instances across multiple calls from multiple
     * threads, those instances have to be thread-safe as well.
     */
    T newImporter(int nodeOffset, PrimitiveIntIterable nodeIds);
}
