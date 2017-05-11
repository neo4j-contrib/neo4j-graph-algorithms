package org.neo4j.graphalgo.core.utils.container;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

/**
 * @author mknblch
 */
public interface Buckets {

    boolean isEmpty();

    void add(int nodeId, int bucket);

    int nextNonEmptyBucket();

    void forEachInBucket(int bucket, IntPredicate consumer);
}
