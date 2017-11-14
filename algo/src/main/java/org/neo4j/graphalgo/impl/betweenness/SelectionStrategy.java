package org.neo4j.graphalgo.impl.betweenness;

import java.util.function.IntConsumer;

/**
 * @author mknblch
 */
public interface SelectionStrategy {

    void forEachNode(IntConsumer nodeConsumer);
}
