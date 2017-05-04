package org.neo4j.graphalgo.api;

import org.neo4j.collection.primitive.PrimitiveIntIterable;

import java.util.Collection;

/**
 * Iterate over each graph-nodeId in batches.
 *
 * @author knutwalker
 */
public interface BatchNodeIterable {

    /**
     * @return a collection of iterables over every node, partitioned by
     *         the given batch size.
     */
    Collection<PrimitiveIntIterable> batchIterables(int batchSize);
}
